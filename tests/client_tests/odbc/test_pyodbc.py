import os
import unittest

import pyodbc
from crate.qa.tests import NodeProvider


class PyODBCTestCase(NodeProvider, unittest.TestCase):

    DRIVER_NAME = os.environ.get('ODBC_DRIVER_NAME', 'PostgreSQL')

    def test_basic_statements(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()

        database = 'doc'
        user = 'crate'
        host = node.addresses.psql.host
        port = node.addresses.psql.port
        driver_name = '{' + self.DRIVER_NAME + '}'

        connection_str = f'DRIVER={driver_name};SERVER={host};PORT={port};DATABASE={database};USERNAME={user}'
        with pyodbc.connect(connection_str) as conn:
            conn.autocommit = True

            with conn.cursor() as cursor:

                cursor.execute("SELECT name FROM sys.cluster")
                row = cursor.fetchone()
                self.assertIsNotNone(row)

                cursor.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
                self.assertEqual(cursor.rowcount, 1)

                cursor.execute("INSERT INTO t1(id) VALUES(?)", 1)

                cursor.execute("REFRESH TABLE t1")

                cursor.execute("UPDATE t1 SET x = ?", 2)

                cursor.execute("REFRESH TABLE t1")

                cursor.execute("DELETE FROM t1 WHERE x = ?", 2)

                cursor.execute("REFRESH TABLE t1")

                cursor.execute("INSERT INTO t1 (id) (SELECT col1 FROM unnest([1, 2]) WHERE col1 = ?)", 1)

                cursor.execute("REFRESH TABLE t1")

                cursor.execute("INSERT INTO t1 (id)"
                               " (SELECT col1 FROM unnest([1, 2]) WHERE col1 = ?)"
                               " ON DUPLICATE KEY UPDATE x = ?", 1, 3)

                cursor.execute("REFRESH TABLE t1")

                cursor.execute("SELECT id, x FROM t1")
                self.assertEqual(cursor.rowcount, 1)
                rows = cursor.fetchall()
                for row in rows:
                    self.assertEqual(row.x, 3)
