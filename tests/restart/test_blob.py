#!/usr/bin/env python3

import unittest
import os
from io import BytesIO
from pathlib import Path
from crate.client import connect
from crate.qa.tests import NodeProvider, wait_for_active_shards


class BlobTestCase(NodeProvider, unittest.TestCase):

    CRATE_VERSION = os.environ.get('CRATE_VERSION', 'latest-nightly')

    def test_blob_index(self):
        node = self._new_node(self.CRATE_VERSION)
        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE BLOB TABLE myblobs
            CLUSTERED INTO 1 shards
            WITH (number_of_replicas = 0)
            """)
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            wait_for_active_shards(cursor)
            cursor.execute("""
            SELECT table_name, number_of_shards, number_of_replicas
            FROM information_schema.tables
            WHERE table_schema = 'blob'
            """)
            result = cursor.fetchone()
            self.assertEqual(result[0], 'myblobs')
            self.assertEqual(result[1], 1)
            self.assertEqual(result[2], '0')

    def test_blob_record(self):
        node = self._new_node(self.CRATE_VERSION)
        node.start()
        digest = ''
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE BLOB TABLE myblobs
            CLUSTERED INTO 1 shards
            WITH (number_of_replicas = 0)
            """)
            blob_container = conn.get_blob_container('myblobs')
            digest = blob_container.put(BytesIO(b'sample data'))
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            wait_for_active_shards(cursor)
            cursor.execute("SELECT count(*) FROM blob.myblobs WHERE digest = ?", (digest,))
            result = cursor.fetchone()
            self.assertEqual(result[0], 1)

            blob_container = conn.get_blob_container('myblobs')
            result = blob_container.get(digest)
            self.assertTrue(blob_container.exists(digest))
            self.assertEqual(next(result), b'sample data')
            filepath = Path(self._path_data).glob(f'**/{digest}')
            self.assertTrue(next(filepath).exists())
