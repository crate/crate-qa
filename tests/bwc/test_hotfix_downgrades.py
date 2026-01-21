import unittest
import gzip
from typing import Dict, Any
from crate.qa.tests import NodeProvider, wait_for_active_shards
from crate.client import connect
from urllib.request import urlopen
import json


def init_data(c):
    c.execute(
        """
        create function foo(int)
        returns int
        language javascript
        as 'function foo(x) { return 42 + x; }'
        """
    )
    c.execute('CREATE TABLE tbl (x int) clustered into 2 shards')
    c.execute('INSERT INTO tbl (x) values (?)', (10,))
    c.execute("refresh table tbl")
    c.execute("create view v1 as (select * from tbl)")
    c.execute("create user arthur with (password = 'secret')")
    c.execute("grant dql to arthur")
    c.execute("create table tparted (x int, y as foo(0), p int) clustered into 2 shards partitioned by (p)")
    c.execute("insert into tparted (x, p) values (1, 1)")
    c.execute("refresh table tparted")


def fetch_versions() -> Dict[str, Any]:
    with urlopen('https://cratedb.com/releases.json') as r:
        if r.headers.get('Content-Encoding') == 'gzip':
            with gzip.open(r, 'rt') as r:
                return json.loads(r.read())
        else:
            return json.loads(r.read().decode('utf-8'))


class HotfixDowngradeTest(NodeProvider, unittest.TestCase):

    def _run_downgrades(self, node):
        major, feature, hotfix = node.version
        for i in range(hotfix - 1, -1, -1):
            # Skip downgrading to version 6.1.0 as it had OID serialization issues
            if node.version == (6, 1, 1):
                return
            new_version = (major, feature, i)
            with self.subTest(version=new_version):
                node = self.upgrade_node(node, '.'.join(map(str, new_version)))

                with connect(node.http_url, error_trace=True) as conn:
                    c = conn.cursor()
                    wait_for_active_shards(c, 8)
                    c.execute('SELECT x FROM tbl')
                    xs = [row[0] for row in c.fetchall()]
                    self.assertEqual(xs, [10])

    def test_can_downgrade_latest_testing_within_hotfix_versions(self):
        cluster = self._new_cluster('latest-testing', 2)
        cluster.start()
        node = cluster.node()
        with connect(node.http_url, error_trace=True) as conn:
            init_data(conn.cursor())

        self._run_downgrades(node)
        cluster.stop()

    def test_can_downgrade_unreleased_testing_branch_within_hotfix_versions(self):
        versions = fetch_versions()
        version = versions["testing"]["version"]
        major, minor, hotfix = version.split(".", maxsplit=3)
        cluster = self._new_cluster(f"{major}.{minor}", 2)
        cluster.start()
        node = cluster.node()
        with connect(node.http_url, error_trace=True) as conn:
            init_data(conn.cursor())
        self._run_downgrades(node)
