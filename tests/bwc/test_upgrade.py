import os
import shutil
import threading
import unittest
import time
from uuid import uuid4
from typing import NamedTuple, Iterable, Tuple
from io import BytesIO

from cr8.run_crate import wait_until
from crate.client import connect
from crate.client.exceptions import ProgrammingError
from crate.qa.tests import (
    VersionDef,
    NodeProvider,
    wait_for_active_shards,
    insert_data,
    gen_id,
    prepare_env,
)

from crate.qa.minio_svr import MinioServer, _is_up

UPGRADE_PATHS = (
    (
        VersionDef('4.0.x', False, []),
        VersionDef('4.0', False, []),
        VersionDef('4.1.x', False, []),
        VersionDef('4.2.x', False, []),
        VersionDef('4.2', False, []),
        VersionDef('branch:master', False, [])
    ),
)

CREATE_PARTED_TABLE = '''
CREATE TABLE parted (
    id string primary key,
    version string primary key,
    cols object (dynamic)
) PARTITIONED BY (version) CLUSTERED INTO 1 SHARDS
'''


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
    col_timestamp TIMESTAMP,
    text STRING,
    INDEX text_ft USING FULLTEXT(text) WITH (analyzer=myanalysis)
) CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)
'''

CREATE_BLOB_TABLE = '''
CREATE BLOB TABLE b1
CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)
'''

CREATE_ANALYZER = '''
CREATE ANALYZER myanalysis (
  TOKENIZER whitespace,
  TOKEN_FILTERS (lowercase, kstem),
  CHAR_FILTERS (mymapping WITH (
    type = 'mapping',
    mappings = ['ph=>f', 'qu=>q', 'foo=>bar']
  ))
)
'''


class Statement(NamedTuple):
    stmt: str
    unsupported_versions: Iterable[str]


# Use statements that use different code paths to retrieve the values
SELECT_STATEMENTS = (
    Statement('SELECT _id, _uid, * FROM t1', []),
    Statement('SELECT * FROM t1 WHERE id = 1', []),
    Statement('SELECT * FROM t1 WHERE col_ip > \'127.0.0.1\'', []),
    Statement('''
    SELECT
        COUNT(DISTINCT col_byte),
        COUNT(DISTINCT col_short),
        COUNT(DISTINCT col_int),
        COUNT(DISTINCT col_long),
        COUNT(DISTINCT col_float),
        COUNT(DISTINCT col_double),
        COUNT(DISTINCT col_string),
        COUNT(DISTINCT col_timestamp)
    FROM t1
    ''', []),
    Statement(
        'SELECT COUNT(DISTINCT col_ip) FROM t1',
        ['2.0.x', '2.1.x']
    ),
    Statement('SELECT id, distance(col_geo_point, [0.0, 0.0]) FROM t1', []),
    Statement('SELECT * FROM t1 WHERE within(col_geo_point, col_geo_shape)', []),
    Statement('SELECT date_trunc(\'week\', col_timestamp), sum(col_int), avg(col_float) FROM t1 GROUP BY 1', []),
    Statement('SELECT _score, text FROM t1 WHERE match(text_ft, \'fase\')', []),
    Statement('UPDATE t1 SET col_int = col_int + 1', []),
)


def run_selects(c, version):
    for stmt in SELECT_STATEMENTS:
        if version in stmt.unsupported_versions:
            continue
        try:
            c.execute(stmt.stmt)
        except ProgrammingError as e:
            raise ProgrammingError('Error executing ' + stmt.stmt) from e


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
    versions = [v.version for v in path]
    return f'{versions[0]} -> {versions[-1]}'


class StorageCompatibilityTest(NodeProvider, unittest.TestCase):

    CLUSTER_SETTINGS = {
        'cluster.name': gen_id(),
    }

    def test_upgrade_paths(self):
        for path in get_test_paths():
            try:
                self.setUp()
                self._test_upgrade_path(path, nodes=3)
            finally:
                self.tearDown()

    def _upgrade(self, cursor, upgrade_segments, num_retries=3):
        """
        Performs the upgrade of the indices and retries in case of
        ProgrammingErrors.

        The retry was added because the wait_for_active shards check
        collects the shard information directly from the nodes. The
        internal ES code, however, retrieves the shard information
        from the ClusterState. A retry is necessary in case the shards
        are ready but the cluster state hasn't been updated yet.
        """
        try:
            if upgrade_segments:
                cursor.execute('OPTIMIZE TABLE doc.t1 WITH (upgrade_segments = true)')
                cursor.execute('OPTIMIZE TABLE blob.b1 WITH (upgrade_segments = true)')
        except ProgrammingError as e:
            print(f'OPTIMIZE failed: {e.message} (num_retries={num_retries})')
            if num_retries > 0 and "PrimaryMissingActionException" in e.message:
                time.sleep(1 / (num_retries + 1))
                self._upgrade(cursor, upgrade_segments, num_retries - 1)
            else:
                raise e

    def _test_upgrade_path(self, versions: Tuple[VersionDef], nodes):
        """ Test upgrade path across specified versions.

        Creates a blob and regular table in first version and inserts a record,
        then goes through all subsequent versions - each time verifying that a
        few simple selects work.
        """
        version_def = versions[0]
        env = prepare_env(version_def.java_home)
        cluster = self._new_cluster(
            version_def.version, nodes, self.CLUSTER_SETTINGS, env)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute(CREATE_ANALYZER)
            c.execute(CREATE_DOC_TABLE)
            c.execute(CREATE_PARTED_TABLE)
            c.execute('''
                INSERT INTO t1 (id, text) VALUES (0, 'Phase queue is foo!')
            ''')
            insert_data(conn, 'doc', 't1', 10)
            c.execute(CREATE_BLOB_TABLE)
            run_selects(c, versions[0].version)
            container = conn.get_blob_container('b1')
            digest = container.put(BytesIO(b'sample data'))
            container.get(digest)
        self._process_on_stop()

        for version_def in versions[1:]:
            self.assert_data_persistence(version_def, nodes, digest)

        # restart with latest version
        version_def = versions[-1]
        self.assert_data_persistence(version_def, nodes, digest)

    def assert_data_persistence(self, version_def, nodes, digest):
        env = prepare_env(version_def.java_home)
        version = version_def.version
        cluster = self._new_cluster(version, nodes, self.CLUSTER_SETTINGS, env)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            wait_for_active_shards(cursor, num_active=0)
            cursor.execute('ALTER TABLE doc.t1 SET ("refresh_interval" = 4000)')
            self._upgrade(cursor, version_def.upgrade_segments, num_retries=5)
            run_selects(cursor, version_def.version)
            container = conn.get_blob_container('b1')
            container.get(digest)
            cursor.execute('ALTER TABLE doc.t1 SET ("refresh_interval" = 2000)')

            # older versions had a bug that caused this to fail
            if version in ('latest-nightly', '3.2'):
                # Test that partition and dynamic columns can be created
                obj = {"t_" + version.replace('.', '_'): True}
                args = (str(uuid4()), version, obj)
                cursor.execute(
                    'INSERT INTO doc.parted (id, version, cols) values (?, ?, ?)',
                    args
                )
        self._process_on_stop()


class MetaDataCompatibilityTest(NodeProvider, unittest.TestCase):

    CLUSTER_SETTINGS = {
        'license.enterprise': 'true',
        'lang.js.enabled': 'true',
        'cluster.name': gen_id(),
    }

    SUPPORTED_VERSIONS = (
        VersionDef('2.3.x', False, []),
        VersionDef('3.3.x', False, []),
        VersionDef('latest-nightly', False, [])
    )

    def test_metadata_compatibility(self):
        nodes = 3

        cluster = self._new_cluster(self.SUPPORTED_VERSIONS[0].version,
                                    nodes,
                                    self.CLUSTER_SETTINGS)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
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

        for version_def in self.SUPPORTED_VERSIONS[1:]:
            self.assert_meta_data(version_def, nodes)

        # restart with latest version
        self.assert_meta_data(self.SUPPORTED_VERSIONS[-1], nodes)

    def assert_meta_data(self, version_def, nodes):
        cluster = self._new_cluster(
            version_def.version,
            nodes,
            self.CLUSTER_SETTINGS,
            prepare_env(version_def.java_home))
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
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
                SELECT class, grantee, ident, state, type
                FROM sys.privileges
                ORDER BY class, grantee, ident, state, type
            ''')
            self.assertEqual([['SCHEMA', 'user_a', 'doc', 'GRANT', 'DDL'],
                              ['SCHEMA', 'user_a', 'doc', 'GRANT', 'DML'],
                              ['SCHEMA', 'user_a', 'doc', 'GRANT', 'DQL']],
                             cursor.fetchall())

            self._process_on_stop()


class DefaultTemplateMetaDataCompatibilityTest(NodeProvider, unittest.TestCase):
    CLUSTER_ID = gen_id()

    CLUSTER_SETTINGS = {
        'cluster.name': CLUSTER_ID,
    }

    SUPPORTED_VERSIONS = (
        VersionDef('3.0.x', False, []),
        VersionDef('latest-nightly', False, [])
    )

    def test_metadata_compatibility(self):
        nodes = 3

        cluster = self._new_cluster(self.SUPPORTED_VERSIONS[0].version,
                                    nodes,
                                    self.CLUSTER_SETTINGS)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("select 1")
        self._process_on_stop()

        for version_def in self.SUPPORTED_VERSIONS[1:]:
            self.assert_dynamic_string_detection(version_def, nodes)

    def assert_dynamic_string_detection(self, version_def, nodes):
        """ Test that a dynamic string column detection works as expected.

        If the cluster was initially created/started with a lower CrateDB
        version, we must ensure that our default template is also upgraded, if
        needed, because it is persisted in the cluster state. That's why
        re-creating tables would not help.
        """
        self._move_nodes_folder_if_needed()
        cluster = self._new_cluster(
            version_def.version,
            nodes,
            self.CLUSTER_SETTINGS,
            prepare_env(version_def.java_home)
        )
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute('CREATE TABLE t1 (o object)')
            cursor.execute('''INSERT INTO t1 (o) VALUES ({"name" = 'foo'})''')
            self.assertEqual(cursor.rowcount, 1)
            cursor.execute('REFRESH TABLE t1')
            cursor.execute("SELECT o['name'], count(*) FROM t1 GROUP BY 1")
            rs = cursor.fetchall()
            self.assertEqual(['foo', 1], rs[0])
            cursor.execute('DROP TABLE t1')
            self._process_on_stop()

    def _move_nodes_folder_if_needed(self):
        """Eliminates the cluster-id folder inside the data directory."""
        data_path_incl_cluster_id = os.path.join(self._path_data, self.CLUSTER_ID)
        if os.path.exists(data_path_incl_cluster_id):
            src_path_nodes = os.path.join(data_path_incl_cluster_id, 'nodes')
            target_path_nodes = os.path.join(self._path_data, 'nodes')
            shutil.move(src_path_nodes, target_path_nodes)
            shutil.rmtree(data_path_incl_cluster_id)


class TableSettingsCompatibilityTest(NodeProvider, unittest.TestCase):

    CLUSTER_SETTINGS = {
        'cluster.name': gen_id(),
    }

    SUPPORTED_VERSIONS = (
        VersionDef('2.3.x', False, []),
        VersionDef('3.2.x', False, [])
    )

    def test_altering_tables_with_old_settings(self):
        """ Test that the settings of tables created with an old not anymore
        supported setting can still be changed when running with the latest
        version. This test ensures that old settings are removed on upgrade or
        at latest when changing some table settings. Before 3.1.2, purging old
        settings was not done correctly and thus altering settings of such
        tables failed.
        """

        nodes = 3

        cluster = self._new_cluster(self.SUPPORTED_VERSIONS[0].version,
                                    nodes,
                                    self.CLUSTER_SETTINGS)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            cursor = conn.cursor()

            # The used setting is only valid until version 2.3.x
            cursor.execute('''
                CREATE TABLE t1 (id int) clustered into 4 shards with ("recovery.initial_shards"=1, number_of_replicas=0);
            ''')
            cursor.execute('''
                CREATE TABLE p1 (id int, p int) clustered into 4 shards partitioned by (p) with ("recovery.initial_shards"=1, number_of_replicas=0);
            ''')
            cursor.execute('''
                INSERT INTO p1 (id, p) VALUES (1, 1);
            ''')
        self._process_on_stop()

        for version_def in self.SUPPORTED_VERSIONS[1:]:
            self.start_cluster_and_alter_tables(version_def, nodes)

    def start_cluster_and_alter_tables(self, version_def, nodes):
        cluster = self._new_cluster(
            version_def.version,
            nodes,
            self.CLUSTER_SETTINGS,
            prepare_env(version_def.java_home)
        )
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            wait_for_active_shards(cursor, 8)
            cursor.execute('''
                ALTER TABLE t1 SET (number_of_replicas=1)
            ''')
            cursor.execute('''
                ALTER TABLE p1 SET (number_of_replicas=1)
            ''')
        self._process_on_stop()


class SnapshotCompatibilityTest(NodeProvider, unittest.TestCase):

    CREATE_REPOSITORY = '''
CREATE REPOSITORY r1 TYPE S3
WITH (access_key = 'minio',
secret_key = 'miniostorage',
bucket='backups',
endpoint = '127.0.0.1:9000',
protocol = 'http')
'''

    CREATE_SNAPSHOT_TPT = "CREATE SNAPSHOT r1.s{} ALL WITH (wait_for_completion = true)"

    RESTORE_SNAPSHOT_TPT = "RESTORE SNAPSHOT r1.s{} ALL WITH (wait_for_completion = true)"

    DROP_DOC_TABLE = 'DROP TABLE t1'

    # This represents an upgrade from a fairly up to date
    # crate 3.3 cluster to the current major version 4.0
    VERSION = ('3.3.x', '4.0')

    def test_snapshot_compatibility(self):
        """Test snapshot compatibility when upgrading 3.3.x -> 4.0.

        Using Minio as a S3 repository, the first cluster that runs
        creates the repo, a table and inserts/selects some data, which
        then is snapshotted and deleted. The next cluster recovers the
        data from the last snapshot, performs further inserts/selects,
        to then snapshot the data and delete it.

        We are interested in the transition 3.3.x -> 4.0
        """
        with MinioServer() as minio:
            t = threading.Thread(target=minio.run)
            t.daemon = True
            t.start()
            wait_until(lambda: _is_up('127.0.0.1', 9000))

            num_nodes = 3
            num_docs = 30
            prev_version = None
            num_snapshot = 1
            path_data = 'data_test_snapshot_compatibility'
            cluster_settings = {
                'cluster.name': gen_id(),
                'path.data': path_data
            }
            shutil.rmtree(path_data, ignore_errors=True)
            for version in self.VERSION:
                cluster = self._new_cluster(version, num_nodes, settings=cluster_settings)
                cluster.start()
                with connect(cluster.node().http_url, error_trace=True) as conn:
                    c = conn.cursor()
                    if not prev_version:
                        c.execute(self.CREATE_REPOSITORY)
                        c.execute(CREATE_ANALYZER)
                        c.execute(CREATE_DOC_TABLE)
                        insert_data(conn, 'doc', 't1', num_docs)
                    else:
                        c.execute(self.RESTORE_SNAPSHOT_TPT.format(num_snapshot - 1))
                    c.execute('SELECT COUNT(*) FROM t1')
                    rowcount = c.fetchone()[0]
                    self.assertEqual(rowcount, num_docs)
                    run_selects(c, version)
                    c.execute(self.CREATE_SNAPSHOT_TPT.format(num_snapshot))
                    c.execute(self.DROP_DOC_TABLE)
                self._process_on_stop()
                prev_version = version
                num_snapshot += 1
            shutil.rmtree(path_data, ignore_errors=True)


class SnapshotHeterogeneousNodesCompatibilityTest(SnapshotCompatibilityTest):

    # Versions after > 4.0.8 contains a fix/change that needs to be compatible with <= 4.0.8
    # See https://github.com/crate/crate/pull/9327
    VERSIONS = ('4.0.x', '4.0.8', '4.0.8')

    def test_snapshot_compatibility(self):
        """Test snapshot compatibility when running a cluster with mixed nodes of versions 4.0 and 4.0.8
        """

        with MinioServer() as minio:
            t = threading.Thread(target=minio.run)
            t.daemon = True
            t.start()
            wait_until(lambda: _is_up('127.0.0.1', 9000))

            num_docs = 30
            path_data = 'data_test_heterogeneous_snapshot_compatibility'
            cluster_settings = {
                'cluster.name': gen_id(),
                'path.data': path_data
            }
            shutil.rmtree(path_data, ignore_errors=True)
            cluster = self._new_heterogeneous_cluster(self.VERSIONS, settings=cluster_settings)
            cluster.start()
            with connect(cluster.node().http_url, error_trace=True) as conn:
                c = conn.cursor()
                c.execute(self.CREATE_REPOSITORY)
                c.execute(CREATE_ANALYZER)
                c.execute(CREATE_DOC_TABLE)
                insert_data(conn, 'doc', 't1', num_docs)
                c.execute('SELECT COUNT(*) FROM t1')
                rowcount = c.fetchone()[0]
                self.assertEqual(rowcount, num_docs)
                run_selects(c, self.VERSION[-1])
                c.execute(self.CREATE_SNAPSHOT_TPT.format(1))
                c.execute(self.DROP_DOC_TABLE)
                c.execute(self.RESTORE_SNAPSHOT_TPT.format(1))
                c.execute('SELECT COUNT(*) FROM t1')
                rowcount = c.fetchone()[0]
                self.assertEqual(rowcount, num_docs)
                run_selects(c, self.VERSION[-1])
            shutil.rmtree(path_data, ignore_errors=True)
        self._process_on_stop()


class ReturningNodesCompatibilityTest(NodeProvider, unittest.TestCase):

    VERSIONS = ('4.1.x', '4.2')

    CLUSTER_SETTINGS = {
        'cluster.name': gen_id(),
    }

    def setUp(self):
        super().setUp()
        self.cluster = self._new_heterogeneous_cluster(self.VERSIONS, self.CLUSTER_SETTINGS)
        self.cluster.start()
        self.node_4_1 = next(x for x in self.cluster._nodes if '4.1' in x.crate_dir)
        self.node_4_2 = next(x for x in self.cluster._nodes if '4.2' in x.crate_dir)

    def tearDown(self):
        self._process_on_stop()
        super().tearDown()

    def test_update_returning_in_mixed_cluster(self):

        """Test backward compatibility when using update with returning clause
           in a mixed cluster of 4.1 and 4.2 nodes. This test ensures that the
           previous functionality from 4.1 for update is fully working when a
           4.2 node is in the cluster. It also ensures that the use of a returning
           clause from a 4.2 node yields a meaningful error when used in a cluster
           with 4.1 nodes.
        """

        with connect(self.node_4_1.http_url, error_trace=True) as conn_4_1:
            cursor = conn_4_1.cursor()
            cursor.execute('CREATE TABLE test (id int primary key, message string) clustered into 2 shards;')
            cursor.execute('''INSERT INTO test VALUES(1, 'msg');''')
            cursor.execute('''UPDATE test SET message='updated' WHERE id= 1;''')
            self.assertEqual(cursor.rowcount, 1)
            with self.assertRaisesRegex(ProgrammingError,
                                        '''SQLActionException\\[SQLParseException: line 1:56: mismatched input \\'returning\\' expecting \\{<EOF>, ';'\\}\\]'''
                                        ):
                cursor.execute('''UPDATE test SET message='msg' WHERE message= 'updated' returning id''')

            with connect(self.node_4_2.http_url, error_trace=True) as conn_latest:
                cursor = conn_latest.cursor()
                cursor.execute('''UPDATE test SET message='updated' WHERE id= 1;''')

                self.assertEqual(cursor.rowcount, 1)
                with self.assertRaisesRegex(ProgrammingError, 'SQLActionException\\[UnsupportedFeatureException: '
                                                              'Returning clause for Update is only supported when all '
                                                              'nodes in the cluster running at least version 4.2.0\\]'):
                    cursor.execute('''UPDATE test SET message='msg' WHERE message= 'updated' returning id''')

    def test_insert_returning_in_mixed_cluster(self):

        """Test backward compatibility when using insert with returning clause
           in a mixed cluster of 4.1 and 4.2 nodes. This test ensures that the
           previous functionality from 4.1 for insert is fully working when a
           4.2 node is in the cluster. It also ensures that the use of a returning
           clause from a 4.2 node yields a meaningful error when used in a cluster
           with 4.1 nodes.
        """

        with connect(self.node_4_1.http_url, error_trace=True) as conn_4_1:
            cursor = conn_4_1.cursor()
            cursor.execute('CREATE TABLE test (id int primary key, message string) clustered into 2 shards;')
            self.assertEqual(cursor.rowcount, 1)
            with self.assertRaisesRegex(ProgrammingError,
                                        '''SQLActionException\\[SQLParseException: line 1:35: mismatched input \\'returning\\' expecting \\{<EOF>, ';'\\}\\]'''
                                        ):
                cursor.execute('''INSERT INTO test VALUES(1, 'msg') returning id''')

            with connect(self.node_4_2.http_url, error_trace=True) as conn_latest:
                cursor = conn_latest.cursor()
                cursor.execute('''INSERT INTO test VALUES(1, 'msg')''')

                self.assertEqual(cursor.rowcount, 1)
                with self.assertRaisesRegex(ProgrammingError, 'SQLActionException\\[UnsupportedFeatureException: '
                                                              'Returning clause for Insert is only supported when all '
                                                              'nodes in the cluster running at least version 4.2.0\\]'):
                    cursor.execute('''INSERT INTO test VALUES(2, 'msg') returning id''')
