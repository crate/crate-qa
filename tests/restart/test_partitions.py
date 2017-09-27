import unittest
import http.client
import json
from crate.client import connect
from crate.qa.tests import NodeProvider


class PartitionTestCase(NodeProvider, unittest.TestCase):

    CRATE_VERSION = 'latest-nightly'
    CRATE_SETTINGS = {
        'es.api.enabled': True
    }

    def test_partioned_table_template(self):
        node = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE parted_table (
                id long,
                title string,
                day timestamp
            ) CLUSTERED BY (title) INTO 1 SHARDS PARTITIONED BY (day)
            WITH (number_of_replicas = 0)
            """)
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT table_name,
            number_of_shards,
            number_of_replicas,
            clustered_by,
            partitioned_by
            FROM information_schema.tables
            WHERE table_name = 'parted_table'
            """)
            result = cursor.fetchone()
            self.assertEqual(result[0], 'parted_table')
            self.assertEqual(result[1], 1)
            self.assertEqual(result[2], '0')
            self.assertEqual(result[3], 'title')
            self.assertEqual(result[4], ['day'])

        conn = http.client.HTTPConnection("localhost", 4200)
        conn.request("GET", "/_template/.partitioned.parted_table.")
        response = conn.getresponse()
        self.assertEqual(response.status, 200)
        template = json.loads(response.read())
        conn.close()
        meta = template['.partitioned.parted_table.']['mappings']['default']['_meta']
        settings = template['.partitioned.parted_table.']['settings']

        self.assertEqual(meta['partitioned_by'], [["day", "date"]])
        self.assertEqual(meta['routing'], 'title')
        self.assertEqual(settings['index']['number_of_shards'], '1')
        self.assertEqual(settings['index']['number_of_replicas'], '0')

    def test_query_partitioned_table(self):
        node = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE parted_table (
                id long,
                title string,
                day timestamp
            ) CLUSTERED BY (title) INTO 1 SHARDS PARTITIONED BY (day)
            WITH (number_of_replicas = 0)
            """)
            cursor.execute("""
            INSERT INTO parted_table (id, title, day)
            VALUES (?, ?, current_timestamp)
            """, (1, 'foo'))
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            self._wait_for_active_shards(cursor)
            cursor.execute("""
            SELECT id, title FROM parted_table
            """)
            result = cursor.fetchone()
            self.assertEqual(result[0], 1)
            self.assertEqual(result[1], 'foo')
