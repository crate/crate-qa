import time
import unittest

from parameterized import parameterized
from crate.client import connect
import random
from random import sample

from crate.qa.tests import NodeProvider, insert_data, UpgradePath

UPGRADE_PATHS = [(UpgradePath('4.2.x', '4.3.x'),), (UpgradePath('4.3.x', 'latest-nightly'),)]
UPGRADE_PATHS_FROM_43 = [(UpgradePath('4.3.x', 'latest-nightly'),)]


class RecoveryTest(NodeProvider, unittest.TestCase):
    NUMBER_OF_NODES = 3
    """
    In depth testing of the recovery mechanism during a rolling restart.
    Based on org.elasticsearch.upgrades.RecoveryIT.java
    """

    def assert_busy(self, assertion, timeout=60, f=2.0):
        waited = 0
        duration = 0.1
        assertion_error = None
        while waited < timeout:
            try:
                assertion()
                return
            except AssertionError as e:
                assertion_error = e
            time.sleep(duration)
            waited += duration
            duration *= f
        raise assertion_error

    def _assert_num_docs_by_node_id(self, conn, schema, table_name, node_id, expected_count):
        c = conn.cursor()
        c.execute('''select num_docs from sys.shards where schema_name = ? and table_name = ? and node['id'] = ?''',
                  (schema, table_name, node_id))
        number_of_docs = c.fetchone()
        self.assertTrue(number_of_docs)
        self.assertEqual(expected_count, number_of_docs[0])

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

    @parameterized.expand(UPGRADE_PATHS)
    def test_recovery_with_concurrent_indexing(self, path):
        """
        This test creates a new table and insert data at every stage of the
        rolling upgrade.
        """
        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas = 2,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                    ''')

            # insert data into the initial homogeneous cluster
            insert_data(conn, 'doc', 'test', 10)

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            # make sure that we can index while the replicas are recovering
            c.execute('''alter table doc.test set ("routing.allocation.enable"='primaries')''')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, self.NUMBER_OF_NODES - 1))
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
            self.assertEqual(len(node_ids), self.NUMBER_OF_NODES)

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            for node_id in node_ids:
                self.assert_busy(lambda: self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 60))

            c.execute('''alter table doc.test set ("routing.allocation.enable"='primaries')''')
            # upgrade the full cluster
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)
            c.execute('''alter table doc.test set ("routing.allocation.enable"='all')''')

            insert_data(conn, 'doc', 'test', 45)
            c.execute('refresh table doc.test')
            c.execute('select count(*) from doc.test')
            res = c.fetchone()
            self.assertEqual(res[0], 105)

            c.execute('select id from sys.nodes')
            node_ids = c.fetchall()
            self.assertEqual(len(node_ids), self.NUMBER_OF_NODES)

            for node_id in node_ids:
                self.assert_busy(lambda: self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 105))

    @parameterized.expand(UPGRADE_PATHS)
    def test_relocation_with_concurrent_indexing(self, path):
        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( "number_of_replicas" = 2,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                        ''')

            insert_data(conn, 'doc', 'test', 10)

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            # make sure that no shards are allocated, so we can make sure the primary stays
            # on the old node (when one node stops, we lose the master too, so a replica
            # will not be promoted)
            c.execute('''alter table doc.test set("routing.allocation.enable"='none')''')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, self.NUMBER_OF_NODES - 1))

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

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            c.execute('''alter table doc.test set ("routing.allocation.include._id"=?)''', (new_node_id, ))
            insert_data(conn, 'doc', 'test', 50)

            # ensure the relocation from old node to new node has occurred; otherwise the table is green
            # even though shards haven't moved to the new node yet (allocation was throttled).
            self.assert_busy(lambda: self._assert_shard_state(conn, 'doc', 'test', new_node_id, 'STARTED'))
            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            c.execute('refresh table doc.test')
            self._assert_num_docs_by_node_id(conn, 'doc', 'test', new_node_id, 60)

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            c.execute('''alter table doc.test set("number_of_replicas"=2)''')
            c.execute('''alter table doc.test reset("routing.allocation.include._id")''')

            insert_data(conn, 'doc', 'test', 45)

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            c.execute('refresh table doc.test')
            c.execute('select id from sys.nodes')
            node_ids = c.fetchall()
            self.assertEqual(len(node_ids), self.NUMBER_OF_NODES)

            for node_id in node_ids:
                self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 105)

    def _assert_shard_state(self, conn, schema, table_name, node_id, state):
        c = conn.cursor()
        c.execute('select current_state from sys.allocations where node_id =? and table_name = ? and table_schema = ?',
                  (node_id, table_name, schema))
        current_state = c.fetchone()
        self.assertTrue(current_state)
        self.assertEqual(current_state[0], state)

    @parameterized.expand(UPGRADE_PATHS)
    def test_recovery(self, path):
        """
        This test creates a new table, insert data and asserts the state at every stage of the
        rolling upgrade.
        """

        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
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
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, self.NUMBER_OF_NODES - 1))

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            if random.choice([True, False]):
                c.execute("refresh table doc.test")

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

    @parameterized.expand(UPGRADE_PATHS)
    def test_recovery_closed_index(self, path):
        """
        This test creates a table in the non upgraded cluster and closes it. It then
        checks that the table is effectively closed and potentially replicated.
        """

        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas = 1,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                      ''')

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            c.execute('alter table doc.test close')

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, self.NUMBER_OF_NODES - 1))

            self._assert_is_closed(conn, 'doc', 'test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            self._assert_is_closed(conn, 'doc', 'test')

    @parameterized.expand(UPGRADE_PATHS)
    def test_closed_index_during_rolling_upgrade(self, path):
        """
        This test creates and closes a new table at every stage of the rolling
        upgrade. It then checks that the table is effectively closed and
        replicated.
        """

        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
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
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, self.NUMBER_OF_NODES - 1))

            self._assert_is_closed(conn, 'doc', 'old_cluster')

            c.execute('''
                      create table doc.mixed_cluster(x int) clustered into 1 shards with( number_of_replicas = 0)
                      ''')

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'mixed_cluster'))
            c.execute('alter table doc.mixed_cluster close')

            self._assert_is_closed(conn, 'doc', 'mixed_cluster')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            self._assert_is_closed(conn, 'doc', 'old_cluster')
            self._assert_is_closed(conn, 'doc', 'mixed_cluster')

            c.execute('''
                      create table doc.upgraded_cluster(x int) clustered into 1 shards with( number_of_replicas = 0)
                      ''')

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'upgraded_cluster'))
            c.execute('alter table doc.upgraded_cluster close')

            self._assert_is_closed(conn, 'doc', 'upgraded_cluster')

    @parameterized.expand(UPGRADE_PATHS)
    def test_update_docs(self, path):
        """
        This test creates a new table, insert data and updates data at every state at every stage of the
        rolling upgrade.
        """
        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
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
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, self.NUMBER_OF_NODES - 1))

            if random.choice([True, False]):
                self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

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
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            updates = [(i, str(random.randint)) for i in range(0, 100)]
            res = c.executemany(
                'insert into doc.test(id, data) values(?, ?) on conflict(id) do update set data = excluded.data',
                updates)
            self.assertEqual(len(res), 100)
            for result in res:
                self.assertEqual(result['rowcount'], 1)

    @parameterized.expand(UPGRADE_PATHS_FROM_43)
    def test_operation_based_recovery(self, path):
        """
        Tests that we should perform an operation-based recovery if there were
        some but not too many uncommitted documents (i.e., less than 10% of
        committed documents or the extra translog) before we upgrade each node.
        This is important when we move from the translog based to retention leases
        based peer recoveries.
        """

        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( "number_of_replicas" = 2,
                        "soft_deletes.enabled" = true)
                        ''')

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            insert_data(conn, 'doc', 'test', random.randint(100, 200))
            c.execute('refresh table doc.test')

            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')
            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            # upgrade to mixed cluster
            self._upgrade_cluster(cluster, path.to_version, random.randint(1, self.NUMBER_OF_NODES - 1))

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)
            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')

    @parameterized.expand(UPGRADE_PATHS_FROM_43)
    def test_turnoff_translog_retention_after_upgraded(self, path):
        """
        Verifies that once all shard copies on the new version, we should turn
        off the translog retention for indices with soft-deletes.
        """

        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            number_of_replicas = random.randint(0, 2)
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas =?,
                        "soft_deletes.enabled" = true)
                     ''', (number_of_replicas, ))

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            insert_data(conn, 'doc', 'test', random.randint(100, 200))
            c.execute('refresh table doc.test')

            num_docs = random.randint(0, 100)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            # update the cluster to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            self.assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
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
