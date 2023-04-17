import os
import shutil
import sys
import threading
import unittest
import subprocess
from uuid import uuid4
from typing import NamedTuple, Iterable, Tuple
from io import BytesIO

from cr8.run_crate import wait_until
from crate.client import connect
from crate.client.connection import Connection
from crate.client.exceptions import ProgrammingError
from crate.qa.tests import (
    VersionDef,
    NodeProvider,
    wait_for_active_shards,
    insert_data,
    gen_id,
    prepare_env, timeout, assert_busy,
)

from crate.qa.minio_svr import MinioServer, _is_up

UPGRADE_PATHS = (
    (
        VersionDef('4.0.x', []),
        VersionDef('4.1.x', []),
        VersionDef('4.2.x', []),
        VersionDef('4.3.x', []),
        VersionDef('4.4.x', []),
        VersionDef('4.5.x', []),
        VersionDef('4.6.x', []),
        VersionDef('4.7.x', []),
        VersionDef('4.8.x', []),
        VersionDef('5.0.x', []),
        VersionDef('5.1.x', []),
        VersionDef('5.2.x', []),
        VersionDef('5.3.x', []),
        VersionDef('latest-nightly', [])
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
    }

    def test_upgrade_paths(self):
        for path in get_test_paths():
            print(f"Upgrade path={[x.version for x in path]}", file=sys.stderr)
            out = subprocess.check_output(["jps"], universal_newlines=True)
            print("Running processes:", file=sys.stderr)
            for line in out.split("\n"):
                print(line, file=sys.stderr)
            try:
                self.setUp()
                self._test_upgrade_path(path, nodes=3)
            finally:
                self.tearDown()

    def _test_upgrade_path(self, versions: Tuple[VersionDef], nodes: int):
        """ Test upgrade path across specified versions.

        Creates a blob and regular table in first version and inserts a record,
        then goes through all subsequent versions - each time verifying that a
        few simple selects work.
        """
        version_def = versions[0]
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

    @timeout(420)
    def _do_upgrade(self, cluster, nodes, paths, versions):
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            assert_busy(lambda: self.assert_nodes(conn, nodes))
            c = conn.cursor()
            c.execute(CREATE_ANALYZER)
            c.execute(CREATE_DOC_TABLE)
            c.execute(CREATE_PARTED_TABLE)
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

        self._process_on_stop()
        for version_def in versions[1:]:
            self.assert_data_persistence(version_def, nodes, digest, paths)
        # restart with latest version
        version_def = versions[-1]
        self.assert_data_persistence(version_def, nodes, digest, paths)

    def assert_data_persistence(self, version_def, nodes, digest, paths):
        env = prepare_env(version_def.java_home)
        version = version_def.version
        print(f"Upgrading to {version}", file=sys.stderr)
        cluster = self._new_cluster(version, nodes, data_paths=paths, settings=self.CLUSTER_SETTINGS, env=env)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            wait_for_active_shards(cursor, 0)
            cursor.execute('ALTER TABLE doc.t1 SET ("refresh_interval" = 4000)')
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
