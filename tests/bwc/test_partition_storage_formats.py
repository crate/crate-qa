import os
import unittest

from collections import defaultdict
from crate.client import connect
from crate.qa.tests import NodeProvider, UpgradePath
from pathlib import Path

UPGRADE_PATH = UpgradePath('5.9.x', 'latest-nightly')


def _add_data(cursor, versions, count):
    # versions = [ old, new ]
    cursor.executemany(
        "INSERT INTO partitioned (version, value) VALUES (?, ?)",
        [(versions[i % len(versions)], i) for i in range(0, count)]
    )


def _partition_paths(cursor):
    paths = defaultdict(list)
    cursor.execute(
        'select p.values, s.path from sys.shards s, information_schema.table_partitions p where s.partition_ident = p.partition_ident;')
    results = cursor.fetchall()
    for (values, path) in results:
        version = values['version']
        paths[version].append(path)
    return paths


def _fdt_size(data_path):
    size = 0
    if not data_path:
        return 0
    for dirpath, dirnames, filenames in os.walk(data_path):
        for filename in filenames:
            path = Path(dirpath, filename)
            ext = path.suffix.lstrip('.')
            if ext == 'fdt':
                filesize = path.stat().st_size
                size += filesize
    return size


class PartitionStorageTest(NodeProvider, unittest.TestCase):

    def test_partition_formats_across_versions(self):
        with self.subTest(repr(UPGRADE_PATH)):
            try:
                self.setUp()
                self._run_tests(UPGRADE_PATH, nodes=3)
            finally:
                self.tearDown()

    def _run_tests(self, upgrade_path, nodes):

        cluster = self._new_cluster(upgrade_path.from_version, nodes)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                CREATE TABLE partitioned (
                    version STRING,
                    value INTEGER
                )
                PARTITIONED BY (version)
            ''')
            _add_data(c, [upgrade_path.from_version], 500)

        for node in cluster:
            self.upgrade_node(node, upgrade_path.to_version)

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()

            # add data across both partitions to ensure that indexing still works in both formats
            _add_data(c, [upgrade_path.to_version], 500)
            _add_data(c, [upgrade_path.from_version, upgrade_path.to_version], 1000)

            # query data across partitions
            c.execute('REFRESH TABLE partitioned')
            c.execute('SELECT _doc FROM partitioned ORDER BY value LIMIT 10')

            c.execute(f"SELECT COUNT(*) FROM partitioned WHERE version='{upgrade_path.from_version}'")
            self.assertEqual(c.fetchone()[0], 1000)
            c.execute(f"SELECT COUNT(*) FROM partitioned WHERE version='{upgrade_path.to_version}'")
            self.assertEqual(c.fetchone()[0], 1000)

            # optimize
            c.execute('OPTIMIZE TABLE partitioned WITH (max_num_segments=1)')
            c.execute('REFRESH TABLE partitioned')

            # check stored field sizes - stored fields for new partition should be much smaller
            partition_paths = _partition_paths(c)
            old_size = sum(_fdt_size(path) for path in partition_paths[upgrade_path.from_version])
            new_size = sum(_fdt_size(path) for path in partition_paths[upgrade_path.to_version])

            self.assertLess(new_size, old_size * 0.25,
                            f'Expected new partition FDT size {new_size} to be less than 25% of old partition {old_size}')
