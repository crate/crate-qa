from crate.client import connect
from crate.qa.tests import NodeProvider
import unittest

class MixedOidsFetchValueTest(NodeProvider, unittest.TestCase):

    def test_mixed_oids(self):
        cluster = self._new_cluster('5.4.x', 3)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute("create table tbl (a text, b text) partitioned by (a)")
            c.execute("insert into tbl (a, b) values ('foo1', 'bar1')")

        for idx, node in enumerate(cluster):
            new_node = self.upgrade_node(node, '5.8.5')
            cluster[idx] = new_node

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute("alter table tbl add column c text")
            c.execute("insert into tbl (a, b, c) values ('foo1', 'bar2', 'baz2')")
            c.execute("insert into tbl (a, b, c) values ('foo2', 'bar1', 'baz1')")

        for idx, node in enumerate(cluster):
            new_node = self.upgrade_node(node, '5.9.x')
            cluster[idx] = new_node

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute("insert into tbl (a, b, c) values ('foo1', 'bar3', 'baz3')")
            c.execute("insert into tbl (a, b, c) values ('foo2', 'bar2', 'baz2')")
            c.execute("insert into tbl (a, b, c) values ('foo3', 'bar1', 'baz1')")

        for idx, node in enumerate(cluster):
            new_node = self.upgrade_node(node, '5.10.x')
            cluster[idx] = new_node

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute("insert into tbl (a, b, c) values ('foo1', 'bar4', 'baz4')")
            c.execute("insert into tbl (a, b, c) values ('foo2', 'bar3', 'baz3')")
            c.execute("insert into tbl (a, b, c) values ('foo3', 'bar2', 'baz2')")
            c.execute("insert into tbl (a, b, c) values ('foo4', 'bar1', 'baz1')")

            c.execute("refresh table tbl")

            # LIMIT 10 forces the engine to go via _doc, which would trigger the bug
            # fixed by https://github.com/crate/crate/pull/17819
            c.execute("select b from tbl limit 10")
            result = c.fetchall()
            for row in result:
                self.assertIsNotNone(row[0])
