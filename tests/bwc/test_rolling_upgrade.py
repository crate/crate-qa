import unittest
from crate.client import connect
from crate.client.cursor import Cursor
from crate.client.connection import Connection
from crate.client.exceptions import ProgrammingError
from cr8.run_crate import CrateNode

from crate.qa.tests import NodeProvider, insert_data, wait_for_active_shards, UpgradePath, assert_busy

ROLLING_UPGRADES_V4 = (
    # 4.0.0 -> 4.0.1 -> 4.0.2 don't support rolling upgrades due to a bug
    UpgradePath('4.8.x', '5.0.x'),
)

ROLLING_UPGRADES_V5 = (

    UpgradePath('6.0', '6.1'),
    # UpgradePath('6.0.x', '6.0'),
    # UpgradePath('6.0', '6.1.x'),
    # UpgradePath('6.1.x', '6.1'),
    # UpgradePath('6.1', 'latest-nightly'),
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

    def _test_rolling_upgrade(self, path: UpgradePath, nodes: int):
        """
        Test a rolling upgrade across given versions.
        An initial test cluster is started and then subsequently each node in
        the cluster is upgraded to the new version.
        After each upgraded node a SQL statement is executed that involves all
        nodes in the cluster, in order to check if communication between nodes
        is possible.
        """

        shards = nodes
        replicas = 1
        expected_active_shards = 0

        settings = {
            "transport.netty.worker_count": 16,
            'lang.js.enabled': 'true'
        }
        cluster = self._new_cluster(path.from_version, nodes, settings=settings)
        cluster.start()
        node = cluster.node()
        with connect(node.http_url, error_trace=True) as conn:
            new_shards = init_data(conn, node.version, shards, replicas)
            expected_active_shards += new_shards
            if node.version >= (5, 7, 0):
                remote_cluster = self._new_cluster(path.from_version, 1, settings=settings, explicit_discovery=False)
                remote_cluster.start()
                remote_node = remote_cluster.node()
                with connect(remote_node.http_url, error_trace=True) as remote_conn:
                    new_shards = init_foreign_data_wrapper_data(conn, remote_conn, node.addresses.psql.port, remote_node.addresses.psql.port)
                    expected_active_shards += new_shards
                    if node.version >= (5, 10, 0):
                        new_shards = init_logical_replication_data(self, conn, remote_conn, node.addresses.transport.port, remote_node.addresses.transport.port, expected_active_shards)
                        expected_active_shards += new_shards

        for idx, node in enumerate(cluster):
            # Enforce an old version node be a handler to make sure that an upgraded node can serve 'select *' from an old version node.
            # Otherwise upgraded node simply requests N-1 columns from old version with N columns and it always works.
            # Was a regression for 5.7 <-> 5.8
            with connect(node.http_url, error_trace=True) as old_node_conn:
                c = old_node_conn.cursor()
                c.execute("SELECT * from sys.nodes")
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
                new_shards = self._test_queries_on_new_node(idx, c, node, new_node, nodes, shards, expected_active_shards)
                expected_active_shards += new_shards
                if node.version >= (5, 7, 0):
                    assert remote_node is not None
                    with connect(remote_node.http_url, error_trace=True) as remote_conn:
                        test_foreign_data_wrapper(self, conn, remote_conn)
                        if node.version >= (5, 10, 0):
                            test_logical_replication_queries(self, conn, remote_conn)

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

    def _test_queries_on_new_node(self,
                                  idx: int,
                                  c: Cursor,
                                  old_node: CrateNode,
                                  new_node: CrateNode,
                                  num_nodes: int,
                                  shards_per_partition: int,
                                  current_shards: int) -> int:
        wait_for_active_shards(c, current_shards)
        new_shards = 0

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
        self.assertEqual(res, [
            [1.0, "matchMe title", {"name": "no match name"}],
            [2.0, "no match title", {"name": "matchMe name"}],
        ])

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
        new_shards += shards_per_partition

        # Ensure table/partition versions created are correct
        if old_node.version >= (5, 0, 0):
            c.execute("insert into doc.t2 (a, b) values (?, ?)", [idx, idx])
            c.execute("refresh table t2")
            c.execute("select a, b, c>=1 and c<=2, d>a+b from doc.t2 where a = ?", [idx])
            self.assertEqual(c.fetchall(), [[idx, idx, True, True]])
            old_version = '.'.join(map(str, old_node.version))
            c.execute("select distinct(version['created']) from information_schema.tables where table_name = 't2'")
            self.assertEqual(c.fetchall(), [[old_version]])
            # There was a behavior change in 5.9. After fully upgrading all nodes in the cluster, newly added
            # partitions' version created will follow the upgraded version.
            # E.g., when 5.9 -> 5.10 is completed, the version created for new partitions will be 5.10
            if old_node.version >= (5, 9, 0):
                c.execute("insert into doc.t3 (a, b) values (?, ?)", [idx, idx])
                new_shards += 1
                c.execute("refresh table t3")
                c.execute("select a, b, c>=1 and c<=2, d>a+b from doc.t3 where a = ?", [idx])
                self.assertEqual(c.fetchall(), [[idx, idx, True, True]])
                c.execute("select distinct(version['created']) from information_schema.tables where table_name = 't3'")
                self.assertEqual(c.fetchall(), [[old_version]])
                partition_version = old_version
                if idx == num_nodes - 1:
                    # the partition added after all nodes are upgraded should follow the upgraded(latest) version
                    partition_version = '.'.join(map(str, new_node.version))
                c.execute("select version['created'] from information_schema.table_partitions where table_name = 't3' and values['a'] = ?", [idx])
                self.assertEqual(c.fetchall(), [[partition_version]])
        return new_shards


def init_data(conn: Connection, version: tuple[int, int, int], shards: int, replicas: int) -> int:
    new_shards = 0
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
    new_shards = shards + (shards * replicas)
    c.execute("deny dql on table doc.t1 to arthur")
    c.execute("CREATE VIEW doc.v1 AS SELECT type, title, value FROM doc.t1")
    insert_data(conn, 'doc', 't1', 1000)
    c.execute("INSERT INTO doc.t1 (type, value, title, author) VALUES (1, 1, 'matchMe title', {name='no match name'})")
    c.execute("INSERT INTO doc.t1 (type, value, title, author) VALUES (2, 2, 'no match title', {name='matchMe name'})")
    c.execute("INSERT INTO doc.t1 (title, author, o) VALUES ('prefix_check', {\"dyn_empty_array\" = []}, {\"dyn_ignored_subcol\" = 'hello'})")

    if version >= (5, 0, 0):
        c.execute('''
            create table doc.t2 (
                a int primary key,
                b int not null,
                c int default (random() + 1),
                d generated always as (a + b + c),
                constraint d CHECK (d > a + b)
            ) clustered into 1 shards with (number_of_replicas = 0)
        ''')
        new_shards += 1
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
    new_shards += shards
    return new_shards


def init_foreign_data_wrapper_data(local_conn: Connection, remote_conn: Connection, local_psql_port: int, remote_psql_port: int) -> int:
    assert 5430 <= local_psql_port <= 5440 and 5430 <= remote_psql_port <= 5440

    c = local_conn.cursor()
    rc = remote_conn.cursor()

    c.execute("create table doc.y (a int) clustered into 1 shards with (number_of_replicas=0)")
    rc.execute("create table doc.y (a int) clustered into 1 shards with (number_of_replicas=0)")
    new_shards = 1

    rc.execute(f"CREATE SERVER source FOREIGN DATA WRAPPER jdbc OPTIONS (url 'jdbc:postgresql://localhost:{local_psql_port}/')")
    c.execute(f"CREATE SERVER remote FOREIGN DATA WRAPPER jdbc OPTIONS (url 'jdbc:postgresql://localhost:{remote_psql_port}/')")

    rc.execute("CREATE FOREIGN TABLE doc.remote_y (a int) SERVER source OPTIONS (schema_name 'doc', table_name 'y')")
    c.execute("CREATE FOREIGN TABLE doc.remote_y (a int) SERVER remote OPTIONS (schema_name 'doc', table_name 'y')")

    wait_for_active_shards(c)
    wait_for_active_shards(rc)

    return new_shards


def test_foreign_data_wrapper(self, local_conn: Connection, remote_conn: Connection):
    c = local_conn.cursor()
    rc = remote_conn.cursor()

    rc.execute("select count(a) from doc.remote_y")
    count = rc.fetchall()[0][0]
    c.execute("insert into doc.y values (1)")
    c.execute("refresh table doc.y")
    rc.execute("select count(a) from doc.remote_y")
    self.assertEqual(rc.fetchall()[0][0], count + 1)

    c.execute("select count(a) from doc.remote_y")
    count = c.fetchall()[0][0]
    rc.execute("insert into doc.y values (1)")
    rc.execute("refresh table doc.y")
    c.execute("select count(a) from doc.remote_y")
    self.assertEqual(c.fetchall()[0][0], count + 1)


def init_logical_replication_data(self, local_conn: Connection, remote_conn: Connection, local_transport_port: int, remote_transport_port: int, local_active_shards: int) -> int:
    assert 4300 <= local_transport_port <= 4310 and 4300 <= remote_transport_port <= 4310

    c = local_conn.cursor()
    c.execute("create table doc.x (a int) clustered into 1 shards with (number_of_replicas=0)")
    c.execute("create publication p for table doc.x")

    rc = remote_conn.cursor()
    rc.execute("create table doc.rx (a int) clustered into 1 shards with (number_of_replicas=0)")
    rc.execute("create publication rp for table doc.rx")

    rc.execute(f"create subscription rs connection 'crate://localhost:{local_transport_port}?user=crate&sslmode=sniff' publication p")
    c.execute(f"create subscription s connection 'crate://localhost:{remote_transport_port}?user=crate&sslmode=sniff' publication rp")

    new_shards = 2  # 1 shard for doc.x and another 1 shard for doc.rx
    wait_for_active_shards(rc, new_shards)
    wait_for_active_shards(c, local_active_shards + new_shards)
    assert_busy(lambda: self.assertEqual(num_docs_x(rc), 0))
    assert_busy(lambda: self.assertEqual(num_docs_rx(c), 0))

    return new_shards


def test_logical_replication_queries(self, local_conn: Connection, remote_conn: Connection):
    c = local_conn.cursor()
    rc = remote_conn.cursor()

    # Cannot drop replicated tables
    with self.assertRaises(ProgrammingError):
        rc.execute("drop table doc.x")
        c.execute("drop table doc.rx")

    count = num_docs_x(rc)
    count2 = num_docs_rx(c)

    c.execute("insert into doc.x values (1)")
    c.execute("refresh table doc.x")
    rc.execute("insert into doc.rx values (1)")
    rc.execute("refresh table doc.rx")

    assert_busy(lambda: self.assertEqual(num_docs_x(rc), count + 1))
    assert_busy(lambda: self.assertEqual(num_docs_rx(c), count2 + 1))


def num_docs_x(cursor):
    cursor.execute("select count(*) from doc.x")
    return cursor.fetchall()[0][0]


def num_docs_rx(cursor):
    cursor.execute("select count(*) from doc.rx")
    return cursor.fetchall()[0][0]
