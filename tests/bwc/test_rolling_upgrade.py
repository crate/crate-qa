import unittest
from crate.client import connect
from crate.client.exceptions import ProgrammingError
from cr8.run_crate import parse_version

from crate.qa.tests import NodeProvider, insert_data, wait_for_active_shards, UpgradePath, assert_busy

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
    UpgradePath('5.10.x', '6.0.x'),
    UpgradePath('6.0.x', 'latest-nightly'),
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

            if int(path.from_version.split('.')[0]) >= 5:
                c.execute('''
                    create table doc.t2 (
                        a int primary key,
                        b int not null,
                        c int default (random() + 1),
                        d generated always as (a + b + c),
                        constraint d CHECK (d > a + b)
                    ) clustered into 1 shards with (number_of_replicas = 0)
                ''')
                expected_active_shards += 1
                c.execute('''
                    create table doc.t3 (
                        a int primary key,
                        b int not null,
                        c int default (random() + 1),
                        d generated always as (a + b + c),
                        constraint d CHECK (d > a + b)
                    ) partitioned by (a) clustered into 1 shards with (number_of_replicas = 0)
                ''')

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

                c.execute("REFRESH TABLE doc.t1")
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
                    SELECT value, title, author
                    FROM doc.t1
                    WHERE MATCH(composite_nested_ft, 'matchMe')
                    ORDER BY value
                ''')
                res = c.fetchall()
                print("Results for match query:")
                print(res[0])
                print(res[1])
                self.assertEqual(len(res), 2)
                # only title matches
                self.assertEqual(res[0][1], 'matchMe title')
                self.assertEqual(res[0][2], {'name': 'no match name'})
                # only name matches
                self.assertEqual(res[1][1], 'no match title')
                self.assertEqual(res[1][2], {'name': 'matchMe name'})

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


                # Ensure inserts also work with the primary on the newer node
                c.execute("alter table t1 set (number_of_replicas = 0)")
                c.execute("""
                    select
                        node['id'],
                        (select version['number'] from sys.nodes where id = node['id']) as version
                    from
                        sys.shards
                    where
                        table_name = 't1'
                        and id = 0
                        and primary = true
                        and state = 'STARTED'
                    """
                )
                primary_node_id, primary_version = c.fetchone()
                c.execute(
                    "select id from sys.nodes where id != ? and version['number'] != ? limit 1",
                    [primary_node_id, primary_version]
                )
                node_ids = list(c.fetchall())
                if node_ids and parse_version(primary_version) < new_node.version:
                    alt_node_id = node_ids[0][0]
                    c.execute("ALTER TABLE t1 REROUTE MOVE SHARD 0 FROM ? TO ?", [primary_node_id, alt_node_id])
                    c.execute("alter table t1 set (number_of_replicas = ?)", [replicas])
                    # insert a few records to ensure shard 0 is used
                    c.execute("INSERT INTO doc.t1 (value) VALUES (?)", bulk_parameters=[[42]] * 10)
                else:
                    # this was the last node upgraded to the new version, no node with old version left
                    c.execute("alter table t1 set (number_of_replicas = ?)", [replicas])

                def check_health():
                    c.execute("select health, table_name, underreplicated_shards from sys.health")
                    health_result = list(c.fetchall())
                    for health, table_name, underreplicated in health_result:
                        self.assertEqual(health, "GREEN", f"{table_name} health must be green")
                        self.assertEqual(underreplicated, 0, f"{table_name} must not have any underreplicated shards")

                assert_busy(check_health, timeout=20)


                # Ensure table/partition versions created are correct
                if int(path.from_version.split('.')[0]) >= 5:
                    c.execute("insert into doc.t2(a, b) values (?, ?)", [idx, idx])
                    c.execute("refresh table t2")
                    c.execute("select a, b, c>=1 and c<=2, d>a+b from doc.t2 where a = ?", [idx])
                    self.assertEqual(c.fetchall(), [[idx, idx, True, True]])
                    old_version = '.'.join(map(str, node.version))
                    c.execute("select distinct(version['created']) from information_schema.tables where table_name = 't2'")
                    self.assertEqual(c.fetchall(), [[old_version]])
                    # There was a behavior change in 5.9. After fully upgrading all nodes in the cluster, newly added
                    # partitions' version created will follow the upgraded version.
                    # E.g., when 5.9 -> 5.10 is completed, the version created for new partitions will be 5.10
                    if int(path.from_version.split('.')[1]) >= 9:
                        c.execute("insert into doc.t3(a, b) values (?, ?)", [idx, idx])
                        expected_active_shards += 1
                        c.execute("refresh table t3")
                        c.execute("select a, b, c>=1 and c<=2, d>a+b from doc.t3 where a = ?", [idx])
                        self.assertEqual(c.fetchall(), [[idx, idx, True, True]])
                        c.execute("select distinct(version['created']) from information_schema.tables where table_name = 't3'")
                        self.assertEqual(c.fetchall(), [[old_version]])
                        partition_version = old_version
                        if idx == nodes - 1:
                            # the partition added after all nodes are upgraded should follow the upgraded(latest) version
                            partition_version = '.'.join(map(str, new_node.version))
                        c.execute("select version['created'] from information_schema.table_partitions where table_name = 't3' and values['a'] = ?", [idx])
                        self.assertEqual(c.fetchall(), [[partition_version]])

        # Finally validate that all shards (primaries and replicas) of all partitions are started
        # and writes into the partitioned table while upgrading were successful
        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute("REFRESH TABLE doc.parted")
            wait_for_active_shards(c, expected_active_shards)
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
