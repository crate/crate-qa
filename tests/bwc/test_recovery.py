import unittest

from cr8.run_crate import get_crate, _extract_version
from crate.client import connect
import random
from random import sample

from crate.qa.tests import NodeProvider, insert_data, UpgradePath, assert_busy

UPGRADE_PATHS = [
    UpgradePath('4.2.x', '4.3.x'),
    UpgradePath('4.3.x', '4.4.0'),
    UpgradePath('4.4.x', '4.5.x'),
    UpgradePath('4.5.x', '4.6.x'),
    UpgradePath('4.6.x', '4.7.x'),
    UpgradePath('4.7.x', '4.8.x'),
    UpgradePath('4.8.x', '5.0.x'),
    UpgradePath('5.0.x', '5.1.x'),
    UpgradePath('5.1.x', '5.2.x'),
    UpgradePath('5.2.x', '5.3.x'),
    UpgradePath('5.3.x', '5.4.x'),
    UpgradePath('5.4.x', 'latest-nightly')
]
UPGRADE_PATHS_FROM_43 = [UpgradePath('4.3.x', '4.4.x')]


@unittest.skip('Recovery tests are currently flaky, skip them until fixed')
class RecoveryTest(NodeProvider, unittest.TestCase):
    """
    In depth testing of the recovery mechanism during a rolling restart.
    Based on org.elasticsearch.upgrades.RecoveryIT.java
    """

    NUMBER_OF_NODES = 3

    def _assert_num_docs_by_node_id(self, conn, schema, table_name, node_id, expected_count):
        c = conn.cursor()
        c.execute('''select num_docs from sys.shards where schema_name = ? and table_name = ? and node['id'] = ?''',
                  (schema, table_name, node_id))
        number_of_docs = c.fetchone()
        self.assertTrue(number_of_docs)
        self.assertEqual(expected_count, number_of_docs[0])

    def _assert_is_green(self, conn, schema, table_name):
        return self._assert_health_is(conn, schema, table_name, 'green')

    def _assert_is_yellow(self, conn, schema, table_name):
        return self._assert_health_is(conn, schema, table_name, 'yellow')

    def _assert_health_is(self, conn: connect, schema: str, table_name: str, health: str):
        c = conn.cursor()
        c.execute('select health from sys.health where table_name=? and table_schema=?', (table_name, schema))
        self.assertEqual(c.fetchone()[0], health.upper())

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

    def _fetch_version_tuple(self, version: str) -> tuple:
        crate_dir = get_crate(version)
        return _extract_version(crate_dir)

    def _upgrade_cluster(self, cluster, version: str, nodes: int) -> None:
        assert nodes <= len(cluster._nodes)
        version_tuple = self._fetch_version_tuple(version)
        nodes_to_upgrade = [(i, n) for i, n in enumerate(cluster) if n.version != version_tuple]
        for i, node in sample(nodes_to_upgrade, min(nodes, len(nodes_to_upgrade))):
            new_node = self.upgrade_node(node, version)
            cluster[i] = new_node

    def _upgrade_to_mixed_cluster(self, cluster, version: str) -> None:
        """
        Upgrade to a mixed version cluster by upgrading one node.
        If multiple nodes are upgraded at once, not all shards (replicas) may be able to start (or re-allocate)
        due to different node version.
        See also See https://github.com/crate/crate/blob/master/server/src/main/java/org/elasticsearch/cluster/routing/allocation/decider/NodeVersionAllocationDecider.java
        """
        self._upgrade_cluster(cluster, version, 1)

    def _run_upgrade_paths(self, test, paths):
        for p in paths:
            try:
                self.setUp()
                test(p)
            finally:
                self.tearDown()

    def test_recovery_with_concurrent_indexing(self):
        """
        This test creates a new table and insert data at every stage of the
        rolling upgrade.
        """

        self._run_upgrade_paths(self._test_recovery_with_concurrent_indexing, UPGRADE_PATHS)

    def _test_recovery_with_concurrent_indexing(self, path):
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

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            # make sure that we can index while the replicas are recovering
            c.execute('''alter table doc.test set ("routing.allocation.enable"='primaries')''')

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

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

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            for node_id in node_ids:
                assert_busy(lambda: self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 60))

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
                assert_busy(lambda: self._assert_num_docs_by_node_id(conn, 'doc', 'test', node_id[0], 105))

    def test_relocation_with_concurrent_indexing(self):
        self._run_upgrade_paths(self._test_relocation_with_concurrent_indexing, UPGRADE_PATHS)

    def _test_relocation_with_concurrent_indexing(self, path):
        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( "number_of_replicas" = 2,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                        ''')

            insert_data(conn, 'doc', 'test', 10)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            # make sure that no shards are allocated, so we can make sure the primary stays
            # on the old node (when one node stops, we lose the master too, so a replica
            # will not be promoted)
            c.execute('''alter table doc.test set("routing.allocation.enable"='none')''')

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

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

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            c.execute('''alter table doc.test set ("routing.allocation.include._id"=?)''', (new_node_id, ))
            insert_data(conn, 'doc', 'test', 50)

            # ensure the relocation from old node to new node has occurred; otherwise the table is green
            # even though shards haven't moved to the new node yet (allocation was throttled).
            assert_busy(lambda: self._assert_shard_state(conn, 'doc', 'test', new_node_id, 'STARTED'))
            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            c.execute('refresh table doc.test')
            self._assert_num_docs_by_node_id(conn, 'doc', 'test', new_node_id, 60)

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            c.execute('''alter table doc.test set("number_of_replicas"=2)''')
            c.execute('''alter table doc.test reset("routing.allocation.include._id")''')

            insert_data(conn, 'doc', 'test', 45)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
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

    def test_recovery(self):
        """
        This test creates a new table, insert data and asserts the state at every stage of the
        rolling upgrade.
        """

        self._run_upgrade_paths(self._test_recovery, UPGRADE_PATHS)

    def _test_recovery(self, path):
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

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            if random.choice([True, False]):
                c.execute("refresh table doc.test")

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

    def test_recovery_closed_index(self):
        """
        This test creates a table in the non upgraded cluster and closes it. It then
        checks that the table is effectively closed and potentially replicated.
        """

        self._run_upgrade_paths(self._test_recovery_closed_index, UPGRADE_PATHS)

    def _test_recovery_closed_index(self, path):
        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas = 1,
                        "unassigned.node_left.delayed_timeout" = '100ms', "allocation.max_retries" = '0')
                      ''')

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            c.execute('alter table doc.test close')

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

            self._assert_is_closed(conn, 'doc', 'test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            self._assert_is_closed(conn, 'doc', 'test')

    def test_closed_index_during_rolling_upgrade(self):
        self._run_upgrade_paths(self._test_closed_index_during_rolling_upgrade, UPGRADE_PATHS)

    def _test_closed_index_during_rolling_upgrade(self, path):
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

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

            self._assert_is_closed(conn, 'doc', 'old_cluster')

            c.execute('''
                      create table doc.mixed_cluster(x int) clustered into 1 shards with( number_of_replicas = 0)
                      ''')

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'mixed_cluster'))
            c.execute('alter table doc.mixed_cluster close')

            self._assert_is_closed(conn, 'doc', 'mixed_cluster')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            self._assert_is_closed(conn, 'doc', 'old_cluster')
            self._assert_is_closed(conn, 'doc', 'mixed_cluster')

            c.execute('''
                      create table doc.upgraded_cluster(x int) clustered into 1 shards with( number_of_replicas = 0)
                      ''')

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'upgraded_cluster'))
            c.execute('alter table doc.upgraded_cluster close')

            self._assert_is_closed(conn, 'doc', 'upgraded_cluster')

    def test_update_docs(self):
        """
        This test creates a new table, insert data and updates data at every state at every stage of the
        rolling upgrade.
        """

        self._run_upgrade_paths(self._test_update_docs, UPGRADE_PATHS)

    def _test_update_docs(self, path):
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

            # ensure all shards are active before upgrading a node. otherwise the cluster tries to allocate new
            # replicas if the upgraded node contained the primary, which will fail due to node version allocation rules.
            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

            if random.choice([True, False]):
                assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            # update the data in a mixed cluster
            updates = [(i, str(random.randint)) for i in range(0, 100)]

            res = c.executemany(
                'insert into doc.test(id, data) values(?, ?) on conflict(id) do update set data = excluded.data',
                updates)
            self.assertEqual(len(res), 100)
            for result in res:
                self.assertEqual(result['rowcount'], 1)

            if random.choice([True, False]):
                assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            updates = [(i, str(random.randint)) for i in range(0, 100)]
            res = c.executemany(
                'insert into doc.test(id, data) values(?, ?) on conflict(id) do update set data = excluded.data',
                updates)
            self.assertEqual(len(res), 100)
            for result in res:
                self.assertEqual(result['rowcount'], 1)

    def test_operation_based_recovery(self):
        """
        Tests that we should perform an operation-based recovery if there were
        some but not too many uncommitted documents (i.e., less than 10% of
        committed documents or the extra translog) before we upgrade each node.
        This is important when we move from the translog based to retention leases
        based peer recoveries.
        """

        self._run_upgrade_paths(self._test_operation_based_recovery, UPGRADE_PATHS_FROM_43)

    def _test_operation_based_recovery(self, path):
        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( "number_of_replicas" = 2,
                        "soft_deletes.enabled" = true)
                        ''')

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            insert_data(conn, 'doc', 'test', random.randint(100, 200))
            c.execute('refresh table doc.test')

            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')
            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)
            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            num_docs = random.randint(0, 3)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            self._assert_ensure_checkpoints_are_synced(conn, 'doc', 'test')

    def test_turnoff_translog_retention_after_upgraded(self):
        """
        Verifies that once all shard copies on the new version, we should turn
        off the translog retention for indices with soft-deletes.
        """

        self._run_upgrade_paths(self._test_turnoff_translog_retention_after_upgraded, UPGRADE_PATHS_FROM_43)

    def _test_turnoff_translog_retention_after_upgraded(self, path):
        cluster = self._new_cluster(path.from_version, self.NUMBER_OF_NODES)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            number_of_replicas = random.randint(0, 2)
            c.execute('''
                        create table doc.test(x int) clustered into 1 shards with( number_of_replicas =?,
                        "soft_deletes.enabled" = true)
                     ''', (number_of_replicas, ))

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            insert_data(conn, 'doc', 'test', random.randint(100, 200))
            c.execute('refresh table doc.test')

            num_docs = random.randint(0, 100)
            if num_docs > 0:
                insert_data(conn, 'doc', 'test', num_docs)

            # update the cluster to the new version
            self._upgrade_cluster(cluster, path.to_version, self.NUMBER_OF_NODES)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
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

    def test_auto_expand_indices_during_rolling_upgrade(self):
        """
        This test ensure that allocation filtering rules won't auto-expanding replicas
        during a rolling upgrade unless all nodes are upgraded to 4.4 (latest).

        See https://github.com/elastic/elasticsearch/pull/50361.
        """

        # Todo: Change upgrade path once 4.4 is released, pin to_version to 4.4 then.
        self._run_upgrade_paths(self._test_auto_expand_indices_during_rolling_upgrade, UPGRADE_PATHS_FROM_43)

    def _test_auto_expand_indices_during_rolling_upgrade(self, path):
        number_of_nodes = 3
        cluster = self._new_cluster(path.from_version, number_of_nodes)
        cluster.start()

        # all nodes without the primary
        number_of_replicas = number_of_nodes - 1
        number_of_replicas_with_excluded_node = number_of_replicas - 1

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute('''select id from sys.nodes''')
            node_ids = c.fetchall()
            self.assertEqual(len(node_ids), number_of_nodes)

            c.execute('''create table doc.test(x int) clustered into 1 shards with( "number_of_replicas" = ?)''',
                      (f"0-{number_of_replicas}",))
            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            # exclude one node from allocation, but this won't have any effect as all nodes are on the old version
            c.execute('alter table doc.test set ("routing.allocation.exclude._id" = ?)', (random.choice(node_ids)[0],))

            # check that the replicas expanding automatically to all nodes, even that one is excluded
            assert_busy(lambda: self._assert_number_of_replicas(conn, 'doc', 'test', number_of_replicas))

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

            # health is yellow because the replicas are expanded, but one could not be allocated as the node
            # is excluded by allocation filtering
            assert_busy(lambda: self._assert_is_yellow(conn, 'doc', 'test'))

            # check that the replicas still expanding automatically to all nodes, even that one is excluded
            assert_busy(lambda: self._assert_number_of_replicas(conn, 'doc', 'test', number_of_replicas))

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, number_of_nodes)

            # now that all nodes are on the same version including the path to expand replicas based on the
            # allocation filtering, replicas are expanded only to 1 and the health is green
            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            assert_busy(lambda: self._assert_number_of_replicas(conn, 'doc', 'test', number_of_replicas_with_excluded_node))

    def _assert_number_of_replicas(self, conn, schema, table_name, count):
        c = conn.cursor()
        c.execute('select count(id) from sys.shards where primary=false and table_name = ? and schema_name=?', (table_name, schema))
        number_of_replicas_allocated = c.fetchone()[0]
        self.assertEqual(count, number_of_replicas_allocated)

    def test_retention_leases_established_when_promoting_primary(self):
        self._run_upgrade_paths(self._test_retention_leases_established_when_promoting_primary, UPGRADE_PATHS_FROM_43)

    def _test_retention_leases_established_when_promoting_primary(self, path):
        number_of_nodes = 3
        cluster = self._new_cluster(path.from_version, number_of_nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()

            number_of_shards = random.randint(1, 5)
            number_of_replicas = random.randint(0, 1)

            c.execute(
                '''create table doc.test(x int) clustered into ? shards with(
                    "number_of_replicas" = ?,
                    "soft_deletes.enabled" = false,
                    "allocation.max_retries" = 0,
                    "unassigned.node_left.delayed_timeout" = '100ms'
                    )''',
                (number_of_shards, number_of_replicas,))

            number_of_docs = random.randint(0, 10)
            if number_of_docs > 0:
                insert_data(conn, 'doc', 'test', number_of_docs)

            if random.choice([True, False]):
                c.execute('refresh table doc.test')

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            self._upgrade_to_mixed_cluster(cluster, path.to_version)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            assert_busy(lambda: self._assert_ensure_peer_recovery_retention_leases_renewed_and_synced(conn, 'doc', 'test'))

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, number_of_nodes)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            assert_busy(lambda: self._assert_ensure_peer_recovery_retention_leases_renewed_and_synced(conn, 'doc', 'test'))

    def test_retention_leases_established_when_relocating_primary(self):
        self._run_upgrade_paths(self._test_retention_leases_established_when_relocating_primary, UPGRADE_PATHS_FROM_43)

    def _test_retention_leases_established_when_relocating_primary(self, path):
        number_of_nodes = 3
        cluster = self._new_cluster(path.from_version, number_of_nodes)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()

            number_of_shards = random.randint(1, 5)
            number_of_replicas = random.randint(0, 1)

            c.execute(
                '''create table doc.test(x int) clustered into ? shards with(
                    "number_of_replicas" = ?,
                    "soft_deletes.enabled" = false,
                    "allocation.max_retries" = 0,
                    "unassigned.node_left.delayed_timeout" = '100ms'
                    )''',
                (number_of_shards, number_of_replicas,))

            number_of_docs = random.randint(0, 10)
            if number_of_docs > 0:
                insert_data(conn, 'doc', 'test', number_of_docs)

            if random.choice([True, False]):
                c.execute('refresh table doc.test')

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            self._upgrade_to_mixed_cluster(cluster, path.to_version)
            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))

            #  trigger a primary relocation by excluding the primary from this index
            c.execute('''select node['id'] from sys.shards where primary=true and table_name='test' ''')
            primary_id = c.fetchall()
            self.assertTrue(primary_id)
            c.execute('alter table doc.test set ("routing.allocation.exclude._id" = ?)', primary_id)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            assert_busy(lambda: self._assert_ensure_peer_recovery_retention_leases_renewed_and_synced(conn, 'doc', 'test'))

            # upgrade fully to the new version
            self._upgrade_cluster(cluster, path.to_version, number_of_nodes)

            assert_busy(lambda: self._assert_is_green(conn, 'doc', 'test'))
            assert_busy(lambda: self._assert_ensure_peer_recovery_retention_leases_renewed_and_synced(conn, 'doc', 'test'))

    def _assert_ensure_peer_recovery_retention_leases_renewed_and_synced(self, conn, schema_name, table_name):
        c = conn.cursor()
        c.execute('''select seq_no_stats['global_checkpoint'],
                             seq_no_stats['local_checkpoint'],
                             seq_no_stats['max_seq_no'],
                             retention_leases['leases']['retaining_seq_no']
                         from sys.shards
                         where table_name=? and schema_name=?
                     ''', (table_name, schema_name))
        res = c.fetchall()
        self.assertTrue(res)
        for r in res:
            global_checkpoint = r[0]
            local_checkpoint = r[1]
            max_seq_no = r[2]
            retaining_seq_no = r[3]
            self.assertEqual(global_checkpoint, max_seq_no)
            self.assertEqual(local_checkpoint, max_seq_no)
            self.assertIsNotNone(retaining_seq_no)
            for r_seq in retaining_seq_no:
                self.assertEqual(r_seq, global_checkpoint + 1)
