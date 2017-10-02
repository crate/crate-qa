import os
import time
import unittest
from io import BytesIO
from crate.client import connect
from crate.qa.tests import VersionDef, NodeProvider, wait_for_active_shards
from faker import Faker
from faker.generator import random


fake = Faker()

VERSIONS = (
    VersionDef('0.54.x', False),
    VersionDef('0.55.x', False),
    VersionDef('0.56.x', False),
    VersionDef('0.57.x', False),
    VersionDef('1.0.x', False),
    VersionDef('1.1.x', True),
    VersionDef('2.0.x', False),
    VersionDef('2.1.x', False),
    VersionDef('2.2.x', False),
    VersionDef('latest-nightly', False),
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
) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas = 0)
'''

CREATE_BLOB_TABLE = '''
CREATE BLOB TABLE b1
CLUSTERED INTO 2 SHARDS WITH (number_of_replicas = 0)
'''

INSERT = '''
INSERT INTO t1 (
    id,
    col_bool,
    col_byte,
    col_short,
    col_int,
    col_long,
    col_float,
    col_double,
    col_string,
    col_geo_point,
    col_geo_shape,
    col_ip,
    col_timestamp
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

# Use statements that use different code paths to retrieve the values
SELECT_STATEMENTS = (
    'SELECT * FROM t1',
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

def dummy_generator():
    id = 0
    while True:
        id += 1
        yield (
            id,
            random.getrandbits(1) and True or False,
            random.randint(-2**7, 2**7 - 1),
            random.randint(-2**15, 2**15 - 1),
            random.randint(-2**31, 2**31 - 1),
            random.randint(-2**63, 2**63 - 1),
            random.random(),
            random.random(),
            fake.name(),
            [random.uniform(-180, 180), random.uniform(-90, 90)],
            'POLYGON (( {0} {1}, {2} {3}, {4} {5}, {6} {7}, {0} {1}))'.format(
                random.uniform(-90, 0),
                random.uniform(-45, 0),
                random.uniform(0, 90),
                random.uniform(-45, 0),
                random.uniform(0, 90),
                random.uniform(0, 45),
                random.uniform(-90, 0),
                random.uniform(45, 0),
            ),
            fake.ipv4(),
            fake.date_this_decade()
        )


def run_selects(c, blob_container, digest):
    for stmt in SELECT_STATEMENTS:
        print('  ', stmt)
        c.execute(stmt)
    print('   Getting blob: ', digest)
    blob_container.get(digest)


class BwcTest(NodeProvider, unittest.TestCase):

    def test_upgrade_path(self):
        for versions in [VERSIONS[x:] for x in range(len(VERSIONS) - 1)]:
            try:
                self.setUp()
                self._test_upgrade_path(versions)
            finally:
                self.tearDown()

    def _test_upgrade_path(self, versions):
        """ Test upgrade path across specified versions.

        Creates a blob and regular table in first version and inserts a record,
        then goes through all subsequent versions - each time verifying that a
        few simple selects work.
        """
        version, _ = versions[0]
        print(f'# Test upgrade path from CrateDB version {version}')
        node = self._new_node(version)
        node.start()
        row = dummy_generator()
        with connect(node.http_url) as conn:
            c = conn.cursor()
            c.execute(CREATE_DOC_TABLE)
            for x in range(10):
                c.execute(INSERT, next(row))
            c.execute('REFRESH TABLE t1')
            c.execute(CREATE_BLOB_TABLE)
            container = conn.get_blob_container('b1')
            digest = container.put(BytesIO(b'sample data'))
            run_selects(c, container, digest)
        self._process_on_stop()

        for version, upgrade_segments in versions[1:]:
            node = self._new_node(version)
            node.start()
            with connect(node.http_url) as conn:
                cursor = conn.cursor()
                wait_for_active_shards(cursor)
                if upgrade_segments:
                    cursor.execute('OPTIMIZE TABLE doc.t1 WITH (upgrade_segments = true)')
                    cursor.execute('OPTIMIZE TABLE blob.b1 WITH (upgrade_segments = true)')
                blobs = conn.get_blob_container('b1')
                run_selects(cursor, blobs, digest)
            self._process_on_stop()
