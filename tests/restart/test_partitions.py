import unittest
from datetime import datetime, timedelta
from crate.client import connect
from crate.qa.tests import NodeProvider, wait_for_active_shards


class PartitionTestCase(NodeProvider, unittest.TestCase):

    def test_query_partitioned_table(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE parted_table (
                id long,
                ts timestamp,
                day__generated GENERATED ALWAYS AS date_trunc('day', ts)
            ) CLUSTERED INTO 1 SHARDS PARTITIONED BY (day__generated)
            WITH (number_of_replicas = 0)
            """)
            for x in range(5):
                cursor.execute("""
                INSERT INTO parted_table (id, ts)
                VALUES (?, ?)
                """, (x, datetime.now() - timedelta(days=x)))
        node.stop()

        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            wait_for_active_shards(cursor)
            cursor.execute("""
            SELECT id, date_trunc('day', ts) = day__generated FROM parted_table order by 1
            """)
            for idx, result in enumerate(cursor.fetchall()):
                self.assertEqual(result[0], idx)
                self.assertTrue(result[1])
