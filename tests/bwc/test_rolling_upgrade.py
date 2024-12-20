import unittest
from crate.client import connect
from crate.qa.tests import NodeProvider, insert_data, wait_for_active_shards, UpgradePath

ROLLING_UPGRADES = (
    # 4.0.0 -> 4.0.1 -> 4.0.2 don't support rolling upgrades due to a bug
    UpgradePath('5.7.x', '5.8.x'),
)


class RollingUpgradeTest(NodeProvider, unittest.TestCase):

    def test_rolling_upgrade(self):
        print("")  # force newline for first print
        for path in ROLLING_UPGRADES:
            print(f"From {path.from_version}")
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
        expected_active_shards = shards + shards * replicas

        settings = {
            "transport.netty.worker_count": 16
        }
        cluster = self._new_cluster(path.from_version, nodes, settings=settings)
        cluster.start()
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute("create user arthur with (password = 'secret')")
            c.execute("grant dql to arthur")
            c.execute(f'''
                CREATE TABLE doc.t1 (
                    type BYTE,
                    value FLOAT,
                    title string,
                    author object as (
                        name string
                    ),
                    o object(ignored) as (a int),
                    index composite_nested_ft using fulltext(title, author['name']) with(analyzer = 'stop')
                ) CLUSTERED INTO {shards} SHARDS
                WITH (number_of_replicas={replicas})
            ''')
            c.execute("CREATE VIEW doc.v1 AS SELECT type, title, value FROM doc.t1")
            insert_data(conn, 'doc', 't1', 1000)

            c.execute("INSERT INTO doc.t1 (type, value, title, author) VALUES (1, 1, 'matchMe title', {name='no match name'})")
            c.execute("INSERT INTO doc.t1 (type, value, title, author) VALUES (2, 2, 'no match title', {name='matchMe name'})")

            c.execute("INSERT INTO doc.t1 (title, author, o) VALUES ('prefix_check', {\"dyn_empty_array\" = []}, {\"dyn_ignored_subcol\" = 'hello'})")

            c.execute(f'''
                CREATE TABLE doc.parted (
                    id INT,
                    value INT
                ) CLUSTERED INTO {shards} SHARDS
                PARTITIONED BY (id)
                WITH (number_of_replicas=0, "write.wait_for_active_shards"=1, "warmer.enabled" = true)
            ''')
            c.execute("INSERT INTO doc.parted (id, value) VALUES (1, 1)")
            # Add the shards of the new partition primaries
            expected_active_shards += shards

            c.execute('ALTER TABLE doc.parted RESET ("warmer.enabled")')

        for idx, node in enumerate(cluster):
            # Enforce an old version node be a handler to make sure that an upgraded node can serve 'select *' from an old version node.
            # Otherwise upgraded node simply requests N-1 columns from old version with N columns and it always works.
            # Was a regression for 5.7 <-> 5.8
            with connect(node.http_url, error_trace=True) as old_node_conn:
                c = old_node_conn.cursor()
                c.execute('''
                    SELECT * from sys.nodes
                ''')
                res = c.fetchall()
                self.assertEqual(len(res), 3)

            print(f"    upgrade node {idx} to {path.to_version}")
            new_node = self.upgrade_node(node, path.to_version)

            # Run a query as a user created on an older version (ensure user is read correctly from cluster state, auth works, etc)
            with connect(cluster.node().http_url, username='arthur', password='secret', error_trace=True) as custom_user_conn:
                c = custom_user_conn.cursor()
                wait_for_active_shards(c, expected_active_shards)
                c.execute("SELECT 1")

            cluster[idx] = new_node
            with connect(new_node.http_url, error_trace=True) as conn:
                c = conn.cursor()
                wait_for_active_shards(c, expected_active_shards)

                c.execute("select name from sys.users order by 1")
                self.assertEqual(c.fetchall(), [["arthur"], ["crate"]])

                c.execute("select * from sys.privileges")
                self.assertEqual(c.fetchall(), [["CLUSTER", "arthur", "crate", None, "GRANT", "DQL"]])

                c.execute('''
                    SELECT type, AVG(value)
                    FROM doc.t1
                    GROUP BY type
                ''')
                c.fetchall()
                # Ensure aggregation with different intermediate input works, this was an regression for 4.1 <-> 4.2
                c.execute('''
                    SELECT type, count(distinct value)
                    FROM doc.t1
                    GROUP BY type
                ''')
                c.fetchall()

                # Ensure scalar symbols are working across versions
                c.execute('''
                    SELECT type, value + 1
                    FROM doc.t1
                    WHERE value > 1
                    LIMIT 1
                ''')
                c.fetchone()
                c.execute('SELECT type, value + 1 FROM doc.v1 WHERE value > 1 LIMIT 1')
                c.fetchone()

                # Ensure match queries work. Table level dedicated index column mapping has been changed in 5.4.
                c.execute('''
                    SELECT title, author
                    FROM doc.t1
                    WHERE MATCH(composite_nested_ft, 'matchMe')
                    ORDER BY value
                ''')
                res = c.fetchall()
                self.assertEqual(len(res), 2)
                # only title matches
                self.assertEqual(res[0][0], 'matchMe title')
                self.assertEqual(res[0][1], {'name': 'no match name'})
                # only name matches
                self.assertEqual(res[1][0], 'no match title')
                self.assertEqual(res[1][1], {'name': 'matchMe name'})

                # Dynamically added empty arrays and ignored object sub-columns are indexed with special prefix starting from 5.5
                # Ensure that reading such columns work across all versions.
                # Related to https://github.com/crate/crate/commit/278d45f176e7d1d3215118255cd69afd2d3786ee
                c.execute('''
                    SELECT author, o['dyn_ignored_subcol']
                    FROM doc.t1
                    WHERE title = 'prefix_check'
                ''')
                res = c.fetchall()
                self.assertEqual(len(res), 1)
                self.assertEqual(res[0][0], {'dyn_empty_array': []})
                self.assertEqual(res[0][1], 'hello')

                # Ensure that inserts are working while upgrading
                c.execute(
                    "INSERT INTO doc.t1 (type, value, title, author) VALUES (3, 3, 'some title', {name='nothing to see, move on'})")

                # Ensure that inserts, which will create a new partition, are working while upgrading
                c.execute("INSERT INTO doc.parted (id, value) VALUES (?, ?)", [idx + 10, idx + 10])
                # Add the shards of the new partition primaries
                expected_active_shards += shards

        # Finally validate that all shards (primaries and replicas) of all partitions are started
        # and writes into the partitioned table while upgrading were successful
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()

            # Ensure that inserts, which will create a new partition, are working after upgrade
            c.execute("INSERT INTO doc.parted (id, value) VALUES (?, ?)", [10000, 10000])
            # Add the shards of the new partition primaries
            expected_active_shards += shards

            wait_for_active_shards(c, expected_active_shards)
