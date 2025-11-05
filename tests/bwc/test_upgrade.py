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
        VersionDef('5.4.x', []),
        VersionDef('5.5.x', []),
        VersionDef('5.6.x', []),
        VersionDef('5.7.x', []),
        VersionDef('5.8.x', []),
        VersionDef('5.9.x', []),
        VersionDef('5.10.x', []),
    ),
    (
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
        VersionDef('6.0', []),
        VersionDef('6.1.x', []),
        VersionDef('6.1', []),
        VersionDef('latest-nightly', [])
    )
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


class MetaDataCompatibilityTest(NodeProvider, unittest.TestCase):

    CLUSTER_SETTINGS = {
        'license.enterprise': 'true',
        'lang.js.enabled': 'true',
        'cluster.name': gen_id(),
    }

    SUPPORTED_VERSIONS = (
        VersionDef('https://cdn.crate.io/downloads/releases/nightly/crate-6.2.0-2025-10-27-00-02-186b705.tar.gz', []),
        VersionDef('https://cdn.crate.io/downloads/releases/nightly/crate-6.2.0-2025-10-28-00-02-31d7a23.tar.gz', []),
    )

    def test_metadata_compatibility(self):
        nodes = 2

        cluster = self._new_cluster(
            self.SUPPORTED_VERSIONS[0].version,
            nodes,
            settings=self.CLUSTER_SETTINGS,
            explicit_discovery=False
        )
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

        paths = [node._settings['path.data'] for node in cluster.nodes()]

        for version_def in self.SUPPORTED_VERSIONS[1:]:
            self.assert_meta_data(version_def, nodes, paths)

        # restart with latest version
        self.assert_meta_data(self.SUPPORTED_VERSIONS[-1], nodes, paths)

    def assert_meta_data(self, version_def, nodes, data_paths=None):
        timestamp = datetime.utcnow().isoformat(timespec='seconds')
        print(f"{timestamp} Upgrade to: {version_def.version}")
        cluster = self._new_cluster(
            version_def.version,
            nodes,
            data_paths,
            self.CLUSTER_SETTINGS,
            prepare_env(version_def.java_home),
            explicit_discovery=False
        )
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT name, superuser
                FROM sys.users
                ORDER BY superuser, name;
            ''')
            rs = cursor.fetchall()
            self.assertEqual([['user_a', False], ['crate', True]], rs)
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
            
            self._process_on_stop()
            