import unittest
from io import BytesIO
from crate.client import connect
from crate.client.cursor import Cursor
from crate.client.connection import Connection
from crate.qa.tests import VersionDef, NodeProvider, \
    wait_for_active_shards, insert_data, gen_id

UPGRADE_PATHS = (
    (
        VersionDef('0.54.x', False),
        VersionDef('0.55.x', False),
        VersionDef('0.56.x', False),
        VersionDef('0.57.x', False),
        VersionDef('1.0.x', False),
        VersionDef('1.1.x', True),
        VersionDef('2.0.x', False),
        VersionDef('2.1.x', False),
        VersionDef('2.2.x', False),
        VersionDef('2.3.x', False),
    ),
    (
        VersionDef('2.0.x', False),
        VersionDef('2.1.x', False),
        VersionDef('2.2.x', False),
        VersionDef('2.3.x', True),
        VersionDef('latest-nightly', False),
    )
)


CREATE_DOC_TABLE = '''
CREATE TABLE t1 (
    id INTEGER PRIMARY KEY,
    col_bool BOOLEAN,
    col_byte BYTE,
    col_short SHORT,
    col_int INTEGER,
    col_long LONG,
    col_float FLOAT,
    col_double DOUBLE,
    col_string STRING,
    col_geo_point GEO_POINT,
    col_geo_shape GEO_SHAPE,
    col_ip IP,
    col_timestamp TIMESTAMP
) CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)
'''

CREATE_BLOB_TABLE = '''
CREATE BLOB TABLE b1
CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)
'''

# Use statements that use different code paths to retrieve the values
SELECT_STATEMENTS = (
    'SELECT _id, _uid, * FROM t1',
    'SELECT * FROM t1 WHERE id = 1',
    'SELECT * FROM t1 WHERE col_ip > \'127.0.0.1\'',
    'SELECT COUNT(DISTINCT col_byte),'
    ' COUNT(DISTINCT col_short),'
    ' COUNT(DISTINCT col_int),'
    ' COUNT(DISTINCT col_long),'
    ' COUNT(DISTINCT col_float),'
    ' COUNT(DISTINCT col_double),'
    ' COUNT(DISTINCT col_string),'
    ' COUNT(DISTINCT col_ip),'
    ' COUNT(DISTINCT col_timestamp)'
    ' FROM t1',
    'SELECT id, distance(col_geo_point, [0.0, 0.0]) FROM t1',
    'SELECT * FROM t1 WHERE within(col_geo_point, col_geo_shape)',
    'SELECT date_trunc(\'week\', col_timestamp), sum(col_int), avg(col_float) FROM t1 GROUP BY 1',
)


def run_selects(c, blob_container, digest):
    for stmt in SELECT_STATEMENTS:
        c.execute(stmt)
    blob_container.get(digest)


def get_test_paths():
    """
    Generater for all possible upgrade paths that should be tested.
    """
    for path in UPGRADE_PATHS:
        for versions in (path[x:] for x in range(len(path) - 1)):
            yield versions


def path_repr(path):
    """
    String representation of the upgrade path in the format::

        from_version -> to_version
    """
    versions = [v for v,_ in path]
    return f'{versions[0]} -> {versions[-1]}'


class StorageCompatibilityTest(NodeProvider, unittest.TestCase):

    CLUSTER_SETTINGS = {
        'cluster.name': gen_id(),
    }

    def test_upgrade_paths(self):
        for path in get_test_paths():
            with self.subTest(path_repr(path)):
                try:
                    self.setUp()
                    self._test_upgrade_path(path, nodes=3)
                finally:
                    self.tearDown()

    def _test_upgrade_path(self, versions, nodes):
        """ Test upgrade path across specified versions.

        Creates a blob and regular table in first version and inserts a record,
        then goes through all subsequent versions - each time verifying that a
        few simple selects work.
        """
        cluster = self._new_cluster(versions[0][0], nodes, self.CLUSTER_SETTINGS)
        cluster.start()
        with connect(cluster.node().http_url) as conn:
            c = conn.cursor()
            c.execute(CREATE_DOC_TABLE)
            insert_data(conn, 'doc', 't1', 10)
            c.execute(CREATE_BLOB_TABLE)
            container = conn.get_blob_container('b1')
            digest = container.put(BytesIO(b'sample data'))
            run_selects(c, container, digest)
        self._process_on_stop()

        for version, upgrade_segments in versions[1:]:
            cluster = self._new_cluster(version, nodes, self.CLUSTER_SETTINGS)
            cluster.start()
            with connect(cluster.node().http_url) as conn:
                cursor = conn.cursor()
                wait_for_active_shards(cursor, 6)
                if upgrade_segments:
                    cursor.execute('OPTIMIZE TABLE doc.t1 WITH (upgrade_segments = true)')
                    cursor.execute('OPTIMIZE TABLE blob.b1 WITH (upgrade_segments = true)')
                cursor.execute('ALTER TABLE doc.t1 SET ("refresh_interval" = 4000)')
                blobs = conn.get_blob_container('b1')
                run_selects(cursor, blobs, digest)
                cursor.execute('ALTER TABLE doc.t1 SET ("refresh_interval" = 2000)')
            self._process_on_stop()


class MetaDataCompatibilityTest(NodeProvider, unittest.TestCase):

    CLUSTER_SETTINGS = {
        'license.enterprise': 'true',
        'lang.js.enabled': 'true',
        'cluster.name': gen_id(),
    }

    SUPPORTED_VERSIONS = (
        '2.3.x',
        'latest-nightly',
    )

    def test_metadata_compatibility(self):
        nodes = 3

        cluster = self._new_cluster(self.SUPPORTED_VERSIONS[0],
                                    nodes,
                                    self.CLUSTER_SETTINGS)
        cluster.start()
        with connect(cluster.node().http_url) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE USER user_a;
            ''')
            cursor.execute('''
                GRANT ALL PRIVILEGES ON SCHEMA doc TO user_a;
            ''')
            cursor.execute('''
                CREATE FUNCTION fact(LONG)
                RETURNS LONG
                LANGUAGE JAVASCRIPT
                AS 'function fact(a) { return a < 2 ? 0 : a * (a - 1); }';
            ''')
        self._process_on_stop()

        for version in self.SUPPORTED_VERSIONS[1:]:
            cluster = self._new_cluster(version,
                                        nodes,
                                        self.CLUSTER_SETTINGS)
            cluster.start()
            with connect(cluster.node().http_url) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT name, superuser
                    FROM sys.users
                    ORDER BY superuser, name;
                ''')
                rs = cursor.fetchall()
                self.assertEqual(['user_a', False], rs[0])
                self.assertEqual(['crate', True], rs[1])
                cursor.execute('''
                    SELECT fact(100);
                ''')
                self.assertEqual(9900, cursor.fetchone()[0])
                cursor.execute('''
                    SELECT class, grantee, ident, state, type FROM sys.privileges;
                ''')
                self.assertEqual([['SCHEMA', 'user_a', 'doc', 'GRANT', 'DDL'],
                                  ['SCHEMA', 'user_a', 'doc', 'GRANT', 'DML'],
                                  ['SCHEMA', 'user_a', 'doc', 'GRANT', 'DQL']],
                                 cursor.fetchall())


            self._process_on_stop()
