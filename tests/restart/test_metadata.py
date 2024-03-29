#!/usr/bin/env python3

import unittest
from crate.client import connect
from crate.qa.tests import NodeProvider


class MetadataTestCase(NodeProvider, unittest.TestCase):

    CRATE_SETTINGS = {
        'license.enterprise': True,
        'lang.js.enabled': True
    }

    def test_udf(self):
        (node, _) = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE FUNCTION subtract(integer, integer)
                RETURNS integer
                LANGUAGE JAVASCRIPT
                AS 'function subtract(a,b) { return a - b; }'
            """)
        node.stop()

        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT routine_name FROM information_schema.routines
            WHERE routine_type = 'FUNCTION'
            """)
            result = cursor.fetchone()
            self.assertEqual(result[0], 'subtract')

    def test_user_information(self):
        (node, _) = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE USER user_a")
        node.stop()

        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name, superuser FROM sys.users ORDER BY name")
            result = cursor.fetchall()
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0][0], 'crate')
            self.assertTrue(result[0][1])
            self.assertEqual(result[1][0], 'user_a')
            self.assertFalse(result[1][1])

    def test_user_privileges(self):
        (node, _) = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE USER user_a")
            cursor.execute("CREATE USER user_b")
            cursor.execute("DENY DQL ON SCHEMA sys TO user_b")
            cursor.execute("GRANT ALL PRIVILEGES ON SCHEMA doc TO user_b")
            cursor.execute("DENY DQL ON SCHEMA doc TO user_a")
            cursor.execute("GRANT ALL PRIVILEGES ON SCHEMA sys TO user_a")
        node.stop()

        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT grantee, ident, state, type FROM sys.privileges
            ORDER BY grantee, ident, type
            """)
            result = cursor.fetchall()
            self.assertEqual(len(result), 10)
            expected = [['user_a', 'doc', 'DENY', 'DQL'],
                        ['user_a', 'sys', 'GRANT', 'AL'],
                        ['user_a', 'sys', 'GRANT', 'DDL'],
                        ['user_a', 'sys', 'GRANT', 'DML'],
                        ['user_a', 'sys', 'GRANT', 'DQL'],
                        ['user_b', 'doc', 'GRANT', 'AL'],
                        ['user_b', 'doc', 'GRANT', 'DDL'],
                        ['user_b', 'doc', 'GRANT', 'DML'],
                        ['user_b', 'doc', 'GRANT', 'DQL'],
                        ['user_b', 'sys', 'DENY', 'DQL']]
            self.assertEqual(result, expected)

    def test_views(self):
        (node, _) = self._new_node(self.CRATE_VERSION, settings=self.CRATE_SETTINGS)
        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE s.t1 (
                ts TIMESTAMP,
                day TIMESTAMP GENERATED ALWAYS AS date_trunc('day', ts),
                value float,
                type SHORT
            ) PARTITIONED BY (day)
            WITH (number_of_replicas=0)
            """)
            cursor.execute("""
            CREATE VIEW s.v1 AS
                SELECT ts, value FROM s.t1 WHERE type = 1;
            """)
            cursor.execute("""
            CREATE VIEW s.v2 AS
                SELECT * FROM s.t1 WHERE day > CURRENT_TIMESTAMP - INTERVAL '24' HOUR;
            """)
        node.stop()

        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'VIEW'
            ORDER BY table_schema, table_name
            """)
            result = cursor.fetchall()
            self.assertEqual(result,
                             [['s', 'v1'],
                              ['s', 'v2']])

            cursor.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_name IN ('v1', 'v2')
            ORDER BY table_name, column_name
            """)
            result = cursor.fetchall()
            self.assertEqual(result,
                             [['v1', 'ts'],
                              ['v1', 'value'],
                              ['v2', 'day'],
                              ['v2', 'ts'],
                              ['v2', 'type'],
                              ['v2', 'value']])
