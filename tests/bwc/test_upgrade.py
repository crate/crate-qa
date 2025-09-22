import os
import shutil
import threading
import unittest
from datetime import datetime
from uuid import uuid4
from typing import NamedTuple, Iterable, Tuple
from io import BytesIO

from cr8.run_crate import wait_until
from crate.client import connect
from crate.client.connection import Connection
from crate.client.exceptions import ProgrammingError
from crate.qa.tests import (
    VersionDef,
    CrateCluster,
    NodeProvider,
    wait_for_active_shards,
    insert_data,
    gen_id,
    prepare_env, timeout, assert_busy,
)

from crate.qa.minio_svr import MinioServer, _is_up

UPGRADE_PATHS = (
    # (
    #     VersionDef('4.0.x', []),
    #     VersionDef('4.1.x', []),
    #     VersionDef('4.2.x', []),
    #     VersionDef('4.3.x', []),
    #     VersionDef('4.4.x', []),
    #     VersionDef('4.5.x', []),
    #     VersionDef('4.6.x', []),
    #     VersionDef('4.7.x', []),
    #     VersionDef('4.8.x', []),
    #     VersionDef('5.0.x', []),
    #     VersionDef('5.1.x', []),
    #     VersionDef('5.2.x', []),
    #     VersionDef('5.3.x', []),
    #     VersionDef('5.4.x', []),
    #     VersionDef('5.5.x', []),
    #     VersionDef('5.6.x', []),
    #     VersionDef('5.7.x', []),
    #     VersionDef('5.8.x', []),
    #     VersionDef('5.9.x', []),
    #     VersionDef('5.10.x', []),
    # ),
    # (
    VersionDef('5.0.x', []),
    VersionDef('5.1.x', []),
    VersionDef('5.2.x', []),
    VersionDef('5.3.x', []),
    VersionDef('5.4.x', []),
    VersionDef('5.5.x', []),
    VersionDef('5.6.x', []),
    VersionDef('5.7.x', []),
    VersionDef('5.8.x', []),
    VersionDef('5.9.x', []),
    VersionDef('5.10.x', []),
    VersionDef('6.0.x', []),
    VersionDef('latest-nightly', [])
    # )
)

CREATE_PARTED_TABLE = '''
CREATE TABLE parted (
    id string primary key,
    version string primary key,
    cols object (dynamic)
) PARTITIONED BY (version) CLUSTERED INTO 1 SHARDS
'''

CREATE_DYNAMIC_TABLE = '''
CREATE TABLE dynamic (
    o object
) WITH (column_policy = 'dynamic')
'''

CREATE_DOC_TABLE = '''
CREATE TABLE t1 (
    id STRING PRIMARY KEY,
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
    INDEX text_ft USING FULLTEXT(text) WITH (analyzer='myanalysis')
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
    Generator for all possible upgrade paths that should be tested.
    """
    # for path in UPGRADE_PATHS:
    for versions in (UPGRADE_PATHS[x:] for x in range(len(UPGRADE_PATHS) - 1)):
        yield versions


def path_repr(path: VersionDef) -> str:
    """
    String representation of the upgrade path in the format::

        from_version -> to_version
    """
    versions = [v.version for v in path]
    return f'{versions[0]} -> {versions[-1]}'


class StorageCompatibilityTest(NodeProvider, unittest.TestCase):

    CLUSTER_SETTINGS = {
        'cluster.name': gen_id(),
        "transport.netty.worker_count": 16,
    }

    def test_upgrade_paths(self):
        for path in get_test_paths():
            try:
                self.setUp()
                self._test_upgrade_path(path, nodes=3)
            finally:
                self.tearDown()

    def _test_upgrade_path(self, versions: Tuple[VersionDef, ...], nodes: int):
        """ Test upgrade path across specified versions.

        Creates a blob and regular table in first version and inserts a record,
        then goes through all subsequent versions - each time verifying that a
        few simple selects work.
        """
        version_def = versions[0]
        timestamp = datetime.utcnow().isoformat(timespec='seconds')
        print(f"\n{timestamp} Start version: {version_def.version}")
        env = prepare_env(version_def.java_home)
        cluster = self._new_cluster(
            version_def.version, nodes, settings=self.CLUSTER_SETTINGS, env=env)
        paths = [node._settings['path.data'] for node in cluster.nodes()]
        try:
            self._do_upgrade(cluster, nodes, paths, versions)
        except Exception as e:
            msg = ""
            msg = "\nLogs\n"
            msg += "==============\n"
            for i, node in enumerate(cluster.nodes()):
                msg += f"-------------- node: {i}\n"
                logs_path = node.logs_path
                cluster_name = node.cluster_name
                logfile = os.path.join(logs_path, cluster_name + ".log")
                with open(logfile, "r") as f:
                    logs = f.read()
                    msg += logs
                msg += "\n"
            raise Exception(msg).with_traceback(e.__traceback__)
        finally:
            cluster_name = cluster.nodes()[0].cluster_name
            for node in cluster.nodes():
                logs_path = node.logs_path
                logfile = os.path.join(logs_path, cluster_name + ".log")
                with open(logfile, "a") as f:
                    f.truncate()
                    f.close()

    @timeout(1800)
    def _do_upgrade(self,
                    cluster: CrateCluster,
                    nodes: int,
                    paths: Iterable[str],
                    versions: Tuple[VersionDef, ...]):
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            assert_busy(lambda: self.assert_nodes(conn, nodes))
            c = conn.cursor()

            c.execute(CREATE_ANALYZER)
            c.execute(CREATE_DOC_TABLE)
            c.execute(CREATE_PARTED_TABLE)
            c.execute(CREATE_DYNAMIC_TABLE)

            c.execute("DROP USER IF EXISTS trillian")
            c.execute("CREATE USER trillian")
            c.execute("GRANT DQL ON TABLE t1 TO trillian")

            c.execute('''
                    INSERT INTO t1 (id, text) VALUES (0, 'Phase queue is foo!')
                ''')
            insert_data(conn, 'doc', 't1', 10)
            c.execute(CREATE_BLOB_TABLE)
            assert_busy(lambda: self.assert_green(conn, 'blob', 'b1'))
            run_selects(c, versions[0].version)
            container = conn.get_blob_container('b1')
            digest = container.put(BytesIO(b'sample data'))

            assert_busy(lambda: self.assert_green(conn, 'blob', 'b1'))
            self.assertIsNotNone(container.get(digest))

        accumulated_dynamic_column_names: list[str] = []
        self._process_on_stop()
        for version_def in versions[1:]:
            timestamp = datetime.utcnow().isoformat(timespec='seconds')
            print(f"{timestamp} Upgrade to: {version_def.version}")
            if version_def.version == '5.3.x':
                breakpoint()
            self.assert_data_persistence(version_def, nodes, digest, paths, accumulated_dynamic_column_names)
        # restart with latest version
        version_def = versions[-1]
        self.assert_data_persistence(version_def, nodes, digest, paths, accumulated_dynamic_column_names)

    def assert_data_persistence(self,
                                version_def: VersionDef,
                                nodes: int,
                                digest: str,
                                paths: Iterable[str],
                                accumulated_dynamic_column_names: list[str]):
        env = prepare_env(version_def.java_home)
        version = version_def.version
        cluster = self._new_cluster(version, nodes, data_paths=paths, settings=self.CLUSTER_SETTINGS, env=env)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            assert_busy(lambda: self.assert_nodes(conn, nodes))
            cursor = conn.cursor()
            wait_for_active_shards(cursor, 0)
            version = version_def.version.replace(".", "_")
            cursor.execute(CREATE_DOC_TABLE.replace(
                "CREATE TABLE t1 (",
                f'CREATE TABLE IF NOT EXISTS versioned."t{version}" ('
            ))
            cursor.execute('ALTER TABLE doc.t1 SET ("refresh_interval" = 4000)')
            run_selects(cursor, version_def.version)
            container = conn.get_blob_container('b1')
            container.get(digest)
            cursor.execute('ALTER TABLE doc.t1 SET ("refresh_interval" = 2000)')

            cursor.execute("select name from sys.users order by 1")
            self.assertEqual(cursor.fetchall(), [["crate"], ["trillian"]])

            cursor.execute("select * from sys.privileges")
            self.assertEqual(cursor.fetchall(), [["TABLE", "trillian", "crate", "doc.t1", "GRANT", "DQL"]])

            cursor.execute("select table_name from information_schema.tables where table_schema = 'versioned'")
            tables = [row[0] for row in cursor.fetchall()]
            for table in tables:
                cursor.execute(f'select * from versioned."{table}"')
                cursor.execute(f'insert into versioned."{table}" (id, col_int) values (?, ?)', [str(uuid4()), 1])

            # to trigger `alter` stmt bug(https://github.com/crate/crate/pull/17178) that falsely updated the table's
            # version created setting that resulted in oids instead of column names in resultsets
            cursor.execute('ALTER TABLE doc.dynamic SET ("refresh_interval" = 900)')
            cursor.execute('INSERT INTO doc.dynamic (o) values (?)', [{version: True}])
            cursor.execute('REFRESH TABLE doc.dynamic')
            accumulated_dynamic_column_names.append(version)
            cursor.execute('SELECT o FROM doc.dynamic')
            result = cursor.fetchall()
            for row in result:
                for name in row[0].keys():
                    self.assertIn(name, accumulated_dynamic_column_names)

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

    def assert_green(self, conn: Connection, schema: str, table_name: str):
        c = conn.cursor()
        c.execute('select health from sys.health where table_name=? and table_schema=?', (table_name, schema))
        response = c.fetchone()
        self.assertNotIsInstance(response, type(None))
        self.assertEqual(response[0], 'GREEN')

    def assert_nodes(self, conn: Connection, num_nodes: int):
        c = conn.cursor()
        c.execute("select count(*) from sys.nodes")
        self.assertEqual(c.fetchone()[0], num_nodes)
