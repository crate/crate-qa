import unittest
from typing import NamedTuple
from crate.client import connect
from crate.qa.tests import NodeProvider, insert_data, wait_for_active_shards


class UpgradePath(NamedTuple):
    from_version: str
    to_version: str

    def __repr__(self):
        return f'{self.from_version} -> {self.to_version}'


ROLLING_UPGRADES = (
    UpgradePath('3.1.x', '3.1'),
    UpgradePath('3.1.0', '3.1.x'),
    UpgradePath('3.0.0', '3.0.x'),
    UpgradePath('2.3.0', '2.3.x'),
)


class RollingUpgradeTest(NodeProvider, unittest.TestCase):

    def test_rolling_upgrade(self):
        for path in ROLLING_UPGRADES:
            print(path)
            with self.subTest(repr(path)):
                try:
                    self.setUp()
                    self._test_rolling_upgrade(path, nodes=3)
                finally:
                    self.tearDown()

    def _test_rolling_upgrade(self, path, nodes):
        """
        Test a rolling upgrade across given versions.
        An initial test cluster is started and then subsequently each node in
        the cluster is upgraded to the new version.
        After each upgraded node a SQL statement is executed that involves all
        nodes in the cluster, in order to check if communication between nodes
        is possible.
        """

        shards, replicas = (nodes, 1)

        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute(f'''
                CREATE TABLE doc.t1 (
                    type BYTE,
                    value FLOAT
                ) CLUSTERED INTO {shards} SHARDS
                WITH (number_of_replicas={replicas})
            ''')
            insert_data(conn, 'doc', 't1', 1000)

        for idx, node in enumerate(cluster):
            new_node = self.upgrade_node(node, path.to_version)
            cluster[idx] = new_node
            with connect(new_node.http_url, error_trace=True) as conn:
                c = conn.cursor()
                wait_for_active_shards(c, shards + replicas * shards)
                c.execute(f'''
                    SELECT type, AVG(value)
                    FROM doc.t1
                    GROUP BY type
                ''')
                c.fetchall()
