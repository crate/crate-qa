import time
import unittest
from crate.client import connect
from crate.client.exceptions import ProgrammingError

from crate.qa.tests import NodeProvider, insert_data, wait_for_active_shards, UpgradePath

ROLLING_UPGRADES_V4 = (
    # 4.0.0 -> 4.0.1 -> 4.0.2 don't support rolling upgrades due to a bug
    UpgradePath('4.0.2', '4.0.x'),
    UpgradePath('4.0.x', '4.1.0'),
    UpgradePath('4.1.0', '4.1.x'),
    UpgradePath('4.1.x', '4.2.x'),
    UpgradePath('4.2.x', '4.3.x'),
    UpgradePath('4.3.x', '4.4.x'),
    UpgradePath('4.4.x', '4.5.x'),
    UpgradePath('4.5.x', '4.6.x'),
    UpgradePath('4.6.x', '4.7.x'),
    UpgradePath('4.7.x', '4.8.x'),
    UpgradePath('4.8.x', '5.0.x'),
    UpgradePath('5.0.x', '5.1.x'),
    UpgradePath('5.1.x', '5.2.x'),
    UpgradePath('5.2.x', '5.3.x'),
    UpgradePath('5.3.x', '5.4.x'),
    UpgradePath('5.4.x', '5.5.x'),
    UpgradePath('5.5.x', '5.6.x'),
    UpgradePath('5.6.x', '5.7.x'),
    UpgradePath('5.7.x', '5.8.x'),
    UpgradePath('5.8.x', '5.9.x'),
    UpgradePath('5.9.x', '5.10.x')
)

ROLLING_UPGRADES_V5 = (
    UpgradePath('5.0.x', '5.1.x'),
    UpgradePath('5.1.x', '5.2.x'),
    UpgradePath('5.2.x', '5.3.x'),
    UpgradePath('5.3.x', '5.4.x'),
    UpgradePath('5.4.x', '5.5.x'),
    UpgradePath('5.5.x', '5.6.x'),
    UpgradePath('5.6.x', '5.7.x'),
    UpgradePath('5.7.x', '5.8.x'),
    UpgradePath('5.8.x', '5.9.x'),
    UpgradePath('5.9.x', '5.10.x'),
    UpgradePath('5.10.x', '5.10'),
    UpgradePath('5.10', 'latest-nightly'),
)


class RollingUpgradeTest(NodeProvider, unittest.TestCase):

    def test_rolling_upgrade_4_to_5(self):
        print("")  # force newline for first print
        for path in ROLLING_UPGRADES_V4:
            print(f"From {path.from_version}")
            with self.subTest(repr(path)):
                try:
                    self.setUp()
                    self._test_rolling_upgrade(path, nodes=3)
                finally:
                    self.tearDown()

    def test_rolling_upgrade_5_to_6(self):
        print("")  # force newline for first print
        for path in ROLLING_UPGRADES_V5:
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
            "transport.netty.worker_count": 16,
            'lang.js.enabled': 'true'
        }
        cluster = self._new_cluster(path.from_version, nodes, settings=settings)
        cluster.start()
        replica_cluster = None
        if path.from_version.startswith("5"):
            replica_cluster = self._new_cluster(path.from_version, 1, settings=settings, explicit_discovery=False)
            replica_cluster.start()
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
            c.execute("deny dql on table doc.t1 to arthur")
            c.execute("CREATE VIEW doc.v1 AS SELECT type, title, value FROM doc.t1")
            insert_data(conn, 'doc', 't1', 1000)

            c.execute("INSERT INTO doc.t1 (type, value, title, author) VALUES (1, 1, 'matchMe title', {name='no match name'})")
            c.execute("INSERT INTO doc.t1 (type, value, title, author) VALUES (2, 2, 'no match title', {name='matchMe name'})")

            c.execute("INSERT INTO doc.t1 (title, author, o) VALUES ('prefix_check', {\"dyn_empty_array\" = []}, {\"dyn_ignored_subcol\" = 'hello'})")

            c.execute('''
                CREATE FUNCTION foo(INT)
                RETURNS INT
                LANGUAGE JAVASCRIPT
                AS 'function foo(a) { return a + 1 }';
            ''')
            c.execute(f'''
                CREATE TABLE doc.parted (
                    id INT,
                    value INT,
                    f_value GENERATED ALWAYS AS foo(value)
                ) CLUSTERED INTO {shards} SHARDS
                PARTITIONED BY (id)
                WITH (number_of_replicas=0, "write.wait_for_active_shards"=1)
            ''')
            c.execute("INSERT INTO doc.parted (id, value) VALUES (1, 1)")
            # Add the shards of the new partition primaries
            expected_active_shards += shards

            if path.from_version.startswith("5"):
                c.execute("create table doc.x (a int) clustered into 1 shards with (number_of_replicas=0)")
                expected_active_shards += 1
                c.execute("create publication p for table doc.x")
                with connect(replica_cluster.node().http_url, error_trace=True) as replica_conn:
                    rc = replica_conn.cursor()
                    rc.execute("create table doc.rx (a int) clustered into 1 shards with (number_of_replicas=0)")
                    rc.execute("create publication rp for table doc.rx")
                    rc.execute(f"create subscription rs connection 'crate://localhost:{cluster.node().addresses.transport.port}?user=crate&sslmode=sniff' publication p")
                c.execute(f"create subscription s connection 'crate://localhost:{replica_cluster.node().addresses.transport.port}?user=crate&sslmode=sniff' publication rp")
                expected_active_shards += 1

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
                wait_for_active_shards(c)
                c.execute("SELECT 1")
                # has no privilege
                with self.assertRaises(ProgrammingError):
                    c.execute("EXPLAIN SELECT * FROM doc.t1")
                # has privilege
                c.execute("EXPLAIN SELECT * FROM doc.v1")

            cluster[idx] = new_node
            with connect(new_node.http_url, error_trace=True) as conn:
                c = conn.cursor()
                wait_for_active_shards(c, expected_active_shards)

                c.execute("select name from sys.users order by 1")
                self.assertEqual(c.fetchall(), [["arthur"], ["crate"]])

                c.execute("select * from sys.privileges order by ident")
                self.assertEqual(
                    c.fetchall(),
                    [['TABLE', 'arthur', 'crate', 'doc.t1', 'DENY', 'DQL'],
                     ['CLUSTER', 'arthur', 'crate', None, 'GRANT', 'DQL']])

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

                # skip 5.5 -> 5.6 and later versions, they fail due to https://github.com/crate/crate/issues/17734
                if int(path.to_version.split('.')[1]) < 5:
                    with connect(replica_cluster.node().http_url, error_trace=True) as replica_conn:
                        rc = replica_conn.cursor()
                        wait_for_active_shards(c)
                        wait_for_active_shards(rc)
                        # Ensure publishing to remote cluster works
                        rc.execute("select count(*) from doc.x")
                        count = rc.fetchall()[0][0]
                        c.execute("insert into doc.x values (1)")
                        time.sleep(3)  # replication delay...
                        rc.execute("select count(*) from doc.x")
                        self.assertEqual(rc.fetchall()[0][0], count + 1)
                        # Ensure subscription from remote cluster works
                        c.execute("select count(*) from doc.rx")
                        count = c.fetchall()[0][0]
                        rc.execute("insert into doc.rx values (1)")
                        time.sleep(3)  # replication delay...
                        c.execute("select count(*) from doc.rx")
                        self.assertEqual(c.fetchall()[0][0], count + 1)

        # Finally validate that all shards (primaries and replicas) of all partitions are started
        # and writes into the partitioned table while upgrading were successful
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            wait_for_active_shards(c, expected_active_shards)
            c.execute('''
                REFRESH TABLE doc.parted
            ''')
            c.execute('''
                SELECT count(*)
                FROM doc.parted
            ''')
            res = c.fetchone()
            self.assertEqual(res[0], nodes + 1)

            # Ensure Arthur can be dropped and re-added
            c.execute("drop user arthur")
            c.execute("select * from sys.privileges")
            self.assertEqual(c.fetchall(), [])

            # Ensure view 'v' can be dropped and re-added
            c.execute("DROP VIEW doc.v1")
            c.execute("CREATE VIEW doc.v1 AS SELECT 11")
            c.execute("SELECT * FROM doc.v1")
            self.assertEqual(c.fetchall(), [[11]])
