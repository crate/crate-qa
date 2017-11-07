#!/usr/bin/env python3

import unittest
import os
from crate.client import connect
from crate.qa.tests import NodeProvider

class MetadataTestCase(NodeProvider, unittest.TestCase):

    CRATE_SETTINGS = {
        'license.enterprise': True,
        'lang.js.enabled': True
    }

    def test_udf(self):
        node = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE FUNCTION subtract(integer, integer)
                RETURNS integer
                LANGUAGE JAVASCRIPT
                AS 'function subtract(a,b) { return a - b; }'
            """)
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT routine_name FROM information_schema.routines
            WHERE routine_type = 'FUNCTION'
            """)
            result = cursor.fetchone()
            self.assertEqual(result[0], 'subtract')

    def test_user_information(self):
        node = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE USER user_a")
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name, superuser FROM sys.users ORDER BY name")
            result = cursor.fetchall()
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0][0], 'crate')
            self.assertTrue(result[0][1])
            self.assertEqual(result[1][0], 'user_a')
            self.assertFalse(result[1][1])

    def test_user_privileges(self):
        node = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE USER user_a")
            cursor.execute("CREATE USER user_b")
            cursor.execute("DENY DQL ON SCHEMA sys TO user_b")
            cursor.execute("GRANT ALL PRIVILEGES ON SCHEMA doc TO user_b")
            cursor.execute("DENY DQL ON SCHEMA doc TO user_a")
            cursor.execute("GRANT ALL PRIVILEGES ON SCHEMA sys TO user_a")
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT grantee, ident, state, type FROM sys.privileges
            ORDER BY grantee, ident, type
            """)
            result = cursor.fetchall()
            self.assertEqual(len(result), 8)
            expected = [['user_a', 'doc', 'DENY', 'DQL'],
                        ['user_a', 'sys', 'GRANT', 'DDL'],
                        ['user_a', 'sys', 'GRANT', 'DML'],
                        ['user_a', 'sys', 'GRANT', 'DQL'],
                        ['user_b', 'doc', 'GRANT', 'DDL'],
                        ['user_b', 'doc', 'GRANT', 'DML'],
                        ['user_b', 'doc', 'GRANT', 'DQL'],
                        ['user_b', 'sys', 'DENY', 'DQL']]
            self.assertEqual(result, expected)

    def test_ingestion_rules(self):
        node = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE mqtt_table (
                client_id int PRIMARY KEY,
                topic string,
                payload object(ignored)
            ) WITH (number_of_replicas=0)
            """)
            cursor.execute("""
            CREATE INGEST RULE mqtt_table_rule
            ON mqtt
            WHERE topic like '%temperature%'
            INTO mqtt_table
            """)
        node.stop()

        node.start()
        with connect(node.http_url) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT rule_name, target_table, source_ident
            FROM information_schema.ingestion_rules
            ORDER BY rule_name
            """)
            result = cursor.fetchone()
            expected = ['mqtt_table_rule', 'doc.mqtt_table', 'mqtt']
            self.assertEqual(result, expected)
