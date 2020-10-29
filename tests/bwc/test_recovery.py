import time
import unittest

from crate.client import connect
import random
from random import sample

from crate.qa.tests import NodeProvider, insert_data
from bwc.test_rolling_upgrade import UpgradePath


class RecoveryTest(NodeProvider, unittest.TestCase):
    """
    In depth testing of the recovery mechanism during a rolling restart.
    Based on org.elasticsearch.upgrades.RecoveryIT.java
    """

    def test(self):
        self._run_tests(
            [
                UpgradePath('4.2.x', '4.3.x'),
                UpgradePath('4.3.x', 'latest-nightly'),
            ],
            [
                self._test_relocation_with_concurrent_indexing,
                self._test_recovery,
                self._test_update_docs,
                self._test_recovery_closed_index,
                self._test_closed_index_during_rolling_upgrade,
                self._test_relocation_with_concurrent_indexing,
            ]
        )

    def test_from_4_3(self):
        self._run_tests(
            [
                UpgradePath('4.3.x', 'latest-nightly')
            ],
            [
                self._test_turnoff_translog_retention_after_upgraded,
                self._test_operation_based_recovery,
            ]
        )

    def _run_tests(self, paths, tests):
        for path in paths:
            for test in tests:
                with self.subTest(repr(path)):
                    try:
                        self.setUp()
                        print(f'Run {test.__name__} upgrading versions {path}')
                        test(path, nodes=3)
                    finally:
                        self.tearDown()

    def _assert_num_docs_by_node_id(self, conn, schema, table_name, node_id, expected_count):
        c = conn.cursor()
        c.execute('''select num_docs from sys.shards where schema_name = ? and table_name = ? and node['id'] = ?''',
                  (schema, table_name, node_id))
        number_of_docs = c.fetchone()
        self.assertEqual(number_of_docs[0], expected_count)

    def _assert_is_green(self, conn, schema, table_name):
        c = conn.cursor()
        c.execute('select health from sys.health where table_name=? and table_schema=?', (table_name, schema))
        self.assertEqual(c.fetchone()[0], 'GREEN')

    def _assert_is_closed(self, conn, schema, table_name):
        c = conn.cursor()
        c.execute('select closed from information_schema.tables where table_name=? and table_schema=?', (table_name, schema))
        self.assertTrue(c.fetchone()[0], True)

    def _assert_ensure_checkpoints_are_synced(self, conn, schema_name, table_name):
        c = conn.cursor()
        c.execute('''select seq_no_stats['global_checkpoint'],
                            seq_no_stats['local_checkpoint'],
                            seq_no_stats['max_seq_no']
                        from sys.shards
                        where table_name=? and schema_name=?
                    ''', (table_name, schema_name))
        res = c.fetchall()
        self.assertTrue(res)
        for r in res:
            global_checkpoint = r[0]
            local_checkpoint = r[1]
            max_seq_no = r[2]
            self.assertEqual(global_checkpoint, max_seq_no)
            self.assertEqual(local_checkpoint, max_seq_no)

    def _upgrade_cluster(self, cluster, version, nodes):
        assert nodes <= len(cluster._nodes)
        min_version = min([n.version for n in cluster._nodes])
        nodes_to_upgrade = [(i, n) for i, n in enumerate(cluster) if n.version == min_version]
        for i, node in sample(nodes_to_upgrade, min(nodes, len(nodes_to_upgrade))):
            new_node = self.upgrade_node(node, version)
            cluster[i] = new_node

    def _test_recovery_with_concurrent_indexing(self, path, nodes):
        """
        This test creates a new table and insert data at every stage of the
        rolling upgrade.
        """
        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas = 2,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                    ''')

            # insert data into the initial homogeneous cluster
            insert_data(conn, 'doc', 'test', 10)

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')
            # make sure that we can index while the replicas are recovering
            c.execute('''alter table doc.test set ("routing.allocation.enable"='primaries')''')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, nodes - 1))
            c.execute('''alter table doc.test set ("routing.allocation.enable"='all')''')
            # insert data into a mixed cluster
            insert_data(conn, 'doc', 'test', 50)
            c.execute('refresh table doc.test')
            # make sure that we can index while the replicas are recovering
            c.execute('select count(*) from doc.test')
            self.assertEqual(c.fetchone()[0], 60)
            # check counts for each node individually
            c.execute('select id from sys.nodes')
            node_ids = c.fetchall()
            self.assertEqual(len(node_ids), nodes)

            time.sleep(3)
            for node_id in node_ids:
                self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 60)

            c.execute('''alter table doc.test set ("routing.allocation.enable"='primaries')''')
            # upgrade the full cluster
            self._upgrade_cluster(cluster, path.to_version, nodes)
            c.execute('''alter table doc.test set ("routing.allocation.enable"='all')''')

            insert_data(conn, 'doc', 'test', 45)
            c.execute('refresh table doc.test')
            c.execute('select count(*) from doc.test')
            res = c.fetchone()
            self.assertEqual(res[0], 105)

            c.execute('select id from sys.nodes')
            node_ids = c.fetchall()
            self.assertEqual(len(node_ids), nodes)

            time.sleep(3)
            for node_id in node_ids:
                self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 105)

    def _test_relocation_with_concurrent_indexing(self, path, nodes):

        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( "number_of_replicas" = 2,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                        ''')

            insert_data(conn, 'doc', 'test', 10)

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')
            # make sure that no shards are allocated, so we can make sure the primary stays
            # on the old node (when one node stops, we lose the master too, so a replica
            # will not be promoted)
            c.execute('''alter table doc.test set("routing.allocation.enable"='none')''')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, nodes - 1))

            c.execute('''select id from sys.nodes order by version['number'] desc limit 1''')
            new_node_id = c.fetchone()[0]
            c.execute('''select id from sys.nodes order by version['number'] asc limit 1''')
            old_node_id = c.fetchone()[0]

            # remove the replica and guaranteed the primary is placed on the old node
            c.execute('''alter table doc.test set (
                        "number_of_replicas"=0,
                        "routing.allocation.enable"='all',
                        "routing.allocation.include._id"=?
                        )''', (old_node_id, ))

            self._assert_is_green(conn, 'doc', 'test')

            c.execute('''alter table doc.test set ("routing.allocation.include._id"=?)''', (new_node_id, ))
            insert_data(conn, 'doc', 'test', 50)

            # ensure the relocation from old node to new node has occurred; otherwise the table is green
            # even though shards haven't moved to the new node yet (allocation was throttled).
            time.sleep(3)
            c.execute('select current_state from sys.allocations where node_id =?', (new_node_id,))
            current_state = c.fetchone()[0]
            self.assertEqual(current_state, 'STARTED')
            self._assert_is_green(conn, 'doc', 'test')
            c.execute('refresh table doc.test')
            self._assert_num_docs_by_node_id(conn, 'doc', 'test', new_node_id, 60)

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, nodes)

            c.execute('''alter table doc.test set("number_of_replicas"=2)''')
            c.execute('''alter table doc.test reset("routing.allocation.include._id")''')

            insert_data(conn, 'doc', 'test', 45)

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')
            c.execute('refresh table doc.test')
            time.sleep(5)
            c.execute('select id from sys.nodes')
            node_ids = c.fetchall()
            self.assertEqual(len(node_ids), nodes)

            for node_id in node_ids:
                self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 105)

    def _test_recovery(self, path, nodes):
        """
        This test creates a new table, insert data and asserts the state at every stage of the
        rolling upgrade.
        """

        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas = 1,
                         "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                    ''')

            num_docs = random.randint(0, 10)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            if random.choice([True, False]):
                c.execute("refresh table doc.test")

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, nodes - 1))

            time.sleep(5)
            self._assert_is_green(conn, 'doc', 'test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, nodes)

            if random.choice([True, False]):
                c.execute("refresh table doc.test")

            self._assert_is_green(conn, 'doc', 'test')

    def _test_recovery_closed_index(self, path, nodes):
        """
        This test creates a table in the non upgraded cluster and closes it. It then
        checks that the table is effectively closed and potentially replicated.
        """

        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas = 1,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                      ''')

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')

            c.execute('alter table doc.test close')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, nodes - 1))

            self._assert_is_closed(conn, 'doc', 'test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, nodes)

            self._assert_is_closed(conn, 'doc', 'test')

    def _test_closed_index_during_rolling_upgrade(self, path, nodes):
        """
        This test creates and closes a new table at every stage of the rolling
        upgrade. It then checks that the table is effectively closed and
        replicated.
        """

        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.old_cluster(x int) clustered into 1 shards with( number_of_replicas = 0)
                      ''')

            self._assert_is_green(conn, 'doc', 'old_cluster')
            c.execute('alter table doc.old_cluster close')
            self._assert_is_closed(conn, 'doc', 'old_cluster')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, nodes - 1))

            self._assert_is_closed(conn, 'doc', 'old_cluster')

            c.execute('''
                      create table doc.mixed_cluster(x int) clustered into 1 shards with( number_of_replicas = 0)
                      ''')

            self._assert_is_green(conn, 'doc', 'mixed_cluster')
            c.execute('alter table doc.mixed_cluster close')

            self._assert_is_closed(conn, 'doc', 'mixed_cluster')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, nodes)

            self._assert_is_closed(conn, 'doc', 'old_cluster')
            self._assert_is_closed(conn, 'doc', 'mixed_cluster')

            c.execute('''
                      create table doc.upgraded_cluster(x int) clustered into 1 shards with( number_of_replicas = 0)
                      ''')

            self._assert_is_green(conn, 'doc', 'upgraded_cluster')
            c.execute('alter table doc.upgraded_cluster close')

            self._assert_is_closed(conn, 'doc', 'upgraded_cluster')

    def _test_update_docs(self, path, nodes):
        """
        This test creates a new table, insert data and updates data at every state at every stage of the
        rolling upgrade.
        """
        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                      create table doc.test(id int primary key, data text) clustered into 1 shards with(
                       "unassigned.node_left.delayed_timeout" = '100ms', "number_of_replicas" = 2)
                      ''')

            inserts = [(i, str(random.randint)) for i in range(0, 100)]
            c.executemany('''insert into doc.test(id, data) values (?, ?)''', inserts)

            c.execute('refresh table doc.test')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, nodes - 1))

            if random.choice([True, False]):
                time.sleep(5)
                self._assert_is_green(conn, 'doc', 'test')

            # update the data in a mixed cluster
            updates = [(i, str(random.randint)) for i in range(0, 100)]

            res = c.executemany(
                'insert into doc.test(id, data) values(?, ?) on conflict(id) do update set data = excluded.data',
                updates)
            self.assertEqual(len(res), 100)
            for result in res:
                self.assertEqual(result['rowcount'], 1)

            if random.choice([True, False]):
                c.execute('refresh table doc.test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, nodes)

            updates = [(i, str(random.randint)) for i in range(0, 100)]
            res = c.executemany(
                'insert into doc.test(id, data) values(?, ?) on conflict(id) do update set data = excluded.data',
                updates)
            self.assertEqual(len(res), 100)
            for result in res:
                self.assertEqual(result['rowcount'], 1)

    def _test_operation_based_recovery(self, path, nodes):
        """
        Tests that we should perform an operation-based recovery if there were
        some but not too many uncommitted documents (i.e., less than 10% of
        committed documents or the extra translog) before we upgrade each node.
        This is important when we move from the translog based to retention leases
        based peer recoveries.
        """

        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( "number_of_replicas" = 2,
                        "soft_deletes.enabled" = true)
                        ''')

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')

            insert_data(conn, 'doc', 'test', random.randint(100, 200))
            c.execute('refresh table doc.test')

            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')
            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, nodes - 1))

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')

            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)
            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, nodes)

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')

            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')

    def _test_turnoff_translog_retention_after_upgraded(self, path, nodes):
        """
        Verifies that once all shard copies on the new version, we should turn
        off the translog retention for indices with soft-deletes.
        """

        cluster = self._new_cluster(path.from_version, nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            number_of_replicas = random.randint(0, 2)
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas =?,
                        "soft_deletes.enabled" = true)
                     ''', (number_of_replicas, ))

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')

            insert_data(conn, 'doc', 'test', random.randint(100, 200))
            c.execute('refresh table doc.test')

            num_docs = random.randint(0, 100)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            # update the cluster to the new version
            self._upgrade_cluster(cluster, path.to_version, nodes)

            time.sleep(3)
            self._assert_is_green(conn, 'doc', 'test')
            c.execute('refresh table doc.test')
            self._assert_translog_is_empty(conn, 'doc', 'test')

    def _assert_translog_is_empty(self, conn, schema, table_name):
        c = conn.cursor()
        c.execute('''select translog_stats['number_of_operations'], translog_stats['uncommitted_operations']
                    from sys.shards where table_name=? and schema_name=? ''', (table_name, schema))
        res = c.fetchall()
        self.assertTrue(res)
        for r in res:
            self.assertEqual(r[0], 0)
            self.assertEqual(r[1], 0)
