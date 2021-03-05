
import unittest
from crate.qa.tests import NodeProvider, wait_for_active_shards
from crate.client import connect


class HotfixDowngradeTest(NodeProvider, unittest.TestCase):

    @unittest.skip("""
    This is expected to fail until CrateDB v4.4.4 is out as all
    versions < 4.4.3 won't fully support hotfix version downgrades.
    See https://github.com/crate/crate/pull/11103.
    """)
    def test_latest_testing_can_be_downgraded_within_hotfix_versions(self):
        cluster = self._new_cluster('latest-testing', 2)
        cluster.start()
        node = cluster.node()
        with connect(node.http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('CREATE TABLE tbl (x int)')
            c.execute('INSERT INTO tbl (x) values (?)', (10,))
        major, feature, hotfix = node.version
        for i in range(hotfix, -1, -1):
            new_version = (major, feature, i)
            with self.subTest(version=new_version):
                node = self.upgrade_node(node, '.'.join(map(str, new_version)))

                with connect(node.http_url, error_trace=True) as conn:
                    c = conn.cursor()
                    wait_for_active_shards(c)
                    c.execute('SELECT x FROM tbl')
                    xs = [row[0] for row in c.fetchall()]
                    self.assertEqual(xs, [10])
