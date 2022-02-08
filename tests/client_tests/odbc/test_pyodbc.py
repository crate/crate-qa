import os
import unittest
from contextlib import contextmanager

import pyodbc
from crate.qa.tests import NodeProvider


@contextmanager
def open_db_connection(connection_string):
    connection = pyodbc.connect(connection_string)
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        yield cursor
    except pyodbc.DatabaseError as err:
        raise err
    finally:
        cursor.close()
        connection.close()


class PyODBCTestCase(NodeProvider, unittest.TestCase):

    DRIVER_NAME = os.environ.get('ODBC_DRIVER_NAME', 'PostgreSQL Unicode')

    def connection_str(self, node):
        database = 'doc'
        user = 'crate'
        host = node.addresses.psql.host
        port = node.addresses.psql.port
        driver_name = '{' + self.DRIVER_NAME + '}'

        return f'DRIVER={driver_name};SERVER={host};PORT={port};DATABASE={database};USERNAME={user}'

    def tearDown(self):
        super().tearDown()

    def test_basic_statements(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()

        with open_db_connection(self.connection_str(node)) as cursor:
            cursor.execute("SELECT name FROM sys.cluster")
            row = cursor.fetchone()
            self.assertIsNotNone(row)

        with open_db_connection(self.connection_str(node)) as cursor:
            cursor.execute("CREATE TABLE t1 ("
                           "id INTEGER PRIMARY KEY, x INTEGER, o OBJECT, a ARRAY(INT), t TIMESTAMP)")
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
                           " ON CONFLICT (id) DO UPDATE SET x = ?", 1, 3)

            cursor.execute("REFRESH TABLE t1")

            cursor.execute("SELECT id, x FROM t1")
            self.assertEqual(cursor.rowcount, 1)
            rows = cursor.fetchall()
            for row in rows:
                self.assertEqual(row.x, 3)

        # array support
        with open_db_connection(self.connection_str(node)) as cursor:
            cursor.execute("INSERT INTO t1 (id, a) VALUES(?, ?)", 2, '{10}')
            cursor.execute("REFRESH TABLE t1")
            cursor.execute("SELECT a FROM t1 WHERE id = ?", 2)
            row = cursor.fetchone()
            self.assertEqual('{"10"}', row.a)

        # object support
        with open_db_connection(self.connection_str(node)) as cursor:
            cursor.execute("INSERT INTO t1 (id, o) VALUES(?, ?)", 3, '{"s":"foo"}')
            cursor.execute("REFRESH TABLE t1")
            cursor.execute("SELECT o FROM t1 WHERE id = ?", 3)
            row = cursor.fetchone()
            self.assertEqual('{"s":"foo"}', row.o)

        with open_db_connection(self.connection_str(node)) as cursor:
            cursor.execute("INSERT INTO t1 (id, t) VALUES(?, ?)", 4, '1999-01-08 04:05:06+02')
            cursor.execute("REFRESH TABLE t1")
            cursor.execute("SELECT t FROM t1 WHERE id = ?", 4)
            row = cursor.fetchone()
            self.assertEqual('1999-01-08 02:05:06', row.t.strftime("%Y-%m-%d %H:%M:%S"))
