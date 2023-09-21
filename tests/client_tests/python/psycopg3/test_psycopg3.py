"""
About
=====

Test cases for CrateDB using `psycopg3` [1,2].

Usage
=====
Normally, this will be executed through the main Python test suite
triggered through the toplevel `Jenkinsfile`.

However, to run the `psycopg3` tests only, for example on a CrateDB
instance already provided through Docker, there's an alternative
option which goes along like this::

    # Run CrateDB.
    docker run -it --rm --publish=5432:5432 crate/crate:nightly

    # Run test suite.
    export CRATEDB_URI=postgres://crate@localhost:5432/doc
    python -m unittest discover -vvvf -s tests/client_tests -k test_psycopg3

References
==========
[1] https://www.psycopg.org/psycopg3/docs/
[2] https://github.com/psycopg/psycopg
"""
import os

import psycopg
import psycopg.rows
import unittest

import psycopg_pool

from crate.qa.tests import NodeProvider


async def basic_queries(test, conn: psycopg.AsyncConnection):

    await conn.execute("DROP TABLE IF EXISTS t1")

    result = await conn.execute(
        "CREATE TABLE t1 (x int primary key, y int)")
    test.assertResultCommandEqual(result, 'CREATE 1')

    result = await conn.execute('INSERT INTO t1 (x) VALUES (%s)', [1])
    test.assertResultCommandEqual(result, 'INSERT 0 1')

    result = await conn.execute('REFRESH TABLE t1')
    test.assertResultCommandEqual(result, 'REFRESH 1')

    result = await conn.execute('UPDATE t1 SET y = %s', [2])
    test.assertResultCommandEqual(result, 'UPDATE 1')

    result = await conn.execute('REFRESH TABLE t1')
    test.assertResultCommandEqual(result, 'REFRESH 1')

    result = await conn.execute('DELETE FROM t1 WHERE y = %s', [2])
    test.assertResultCommandEqual(result, 'DELETE 1')

    result = await conn.execute('''
        INSERT INTO t1 (x) (
            SELECT unnest FROM unnest([1, 2]) WHERE unnest = %s
        )
    ''', [1])
    test.assertResultCommandEqual(result, 'INSERT 0 1')

    result = await conn.execute('''
        INSERT INTO t1 (x) (
            SELECT unnest FROM unnest([1, 2]) WHERE unnest = %s)
        ON CONFLICT (x) DO UPDATE SET y = %s
    ''', [1, 2])
    test.assertResultCommandEqual(result, 'INSERT 0 1')


async def record_type_can_be_read_using_binary_streaming(test, conn):
    return unittest.skip("Not sure how to implement with psycopg3")
    result = await conn.fetch('SELECT pg_catalog.pg_get_keywords()')
    keyword = sorted([row[0] for row in result], key=lambda x: x[0])[0]
    test.assertEqual(keyword, ('absolute', 'U', 'unreserved'))


async def bitstring_can_be_inserted_and_selected_using_binary_encoding(test, conn):
    return unittest.skip("Not supported by psycopg3, see https://github.com/psycopg/psycopg/tree/3.1.8/psycopg/psycopg/types")
    """
    xs = asyncpg.BitString('0101')
    await conn.execute('drop table if exists tbl_bit')
    await conn.execute('create table tbl_bit (xs bit(4))')
    await conn.execute('insert into tbl_bit (xs) values (?)', xs)
    await conn.execute('refresh table tbl_bit')
    result = await conn.fetch('select xs from tbl_bit')
    test.assertEqual(result[0][0], xs)
    """


async def float_vector_can_be_inserted_and_selected(test, conn):
    fv = [1.1, 2.2, 3.3, 4.4]
    await conn.execute('drop table if exists tbl_fv')
    await conn.execute('create table tbl_fv (id int, fv float_vector(4))')
    await conn.execute('insert into tbl_fv (id, fv) values (1, [1.1, 2.2, 3.3, 4.4])')
    await conn.execute('insert into tbl_fv (id, fv) values (2, null)')
    await conn.execute('refresh table tbl_fv')
    cur = await conn.execute('select * from tbl_fv order by id')
    result = await cur.fetchall()
    test.assertEqual(result[0][1], fv)
    test.assertEqual(result[1][1], None)


async def fetch_summits_client_cursor(test, uri):
    """
    Use the `cursor.execute` method to acquire results, using a client-side cursor.

    https://www.psycopg.org/psycopg3/docs/advanced/async.html
    https://www.psycopg.org/psycopg3/docs/advanced/cursors.html#client-side-cursors
    """
    conn = await psycopg.AsyncConnection.connect(uri)
    async with conn.transaction():
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            cur = await cursor.execute(
                'select mountain from sys.summits order by height desc')
            first, second = await cur.fetchmany(size=2)
            third, fourth = await cur.fetchmany(size=2)
    await conn.close()
    test.assertEqual(first['mountain'], 'Mont Blanc')
    test.assertEqual(second['mountain'], 'Monte Rosa')
    test.assertEqual(third['mountain'], 'Dom')
    test.assertEqual(fourth['mountain'], 'Liskamm')


async def fetch_summits_server_cursor(test, uri):
    """
    Use the `cursor.execute` method to acquire results, using a server-side cursor.

    https://www.psycopg.org/psycopg3/docs/advanced/async.html
    https://www.psycopg.org/psycopg3/docs/advanced/cursors.html#server-side-cursors
    """
    conn = await psycopg.AsyncConnection.connect(uri)
    async with conn.transaction():
        async with conn.cursor(name="foo", row_factory=psycopg.rows.dict_row) as cursor:
            cur = await cursor.execute(
                'select mountain from sys.summits order by height desc')
            first, second = await cur.fetchmany(size=2)
            third, fourth = await cur.fetchmany(size=2)
    await conn.close()
    test.assertEqual(first['mountain'], 'Mont Blanc')
    test.assertEqual(second['mountain'], 'Monte Rosa')
    test.assertEqual(third['mountain'], 'Dom')
    test.assertEqual(fourth['mountain'], 'Liskamm')


async def fetch_summits_stream(test, uri):
    """
    Use the `cursor.stream` method to acquire results.

    https://www.psycopg.org/psycopg3/docs/advanced/async.html
    https://www.psycopg.org/psycopg3/docs/api/cursors.html#psycopg.AsyncCursor.stream
    """
    records = []
    conn = await psycopg.AsyncConnection.connect(uri)
    async with conn.transaction():
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            gen = cursor.stream(
                'select mountain from sys.summits order by height desc')

            # NOTE: Must exhaust the generator completely.
            #       When using `await anext(cur)`, the program stalls.
            async for record in gen:
                records.append(record)

    await conn.close()
    first, second = records[0:2]
    third, fourth = records[2:4]
    test.assertEqual(first['mountain'], 'Mont Blanc')
    test.assertEqual(second['mountain'], 'Monte Rosa')
    test.assertEqual(third['mountain'], 'Dom')
    test.assertEqual(fourth['mountain'], 'Liskamm')


async def exec_queries_pooled(test, uri):
    pool = psycopg_pool.AsyncConnectionPool(uri)
    async with pool.connection() as conn:
        await basic_queries(test, conn)
        await record_type_can_be_read_using_binary_streaming(test, conn)
        await bitstring_can_be_inserted_and_selected_using_binary_encoding(test, conn)
        await float_vector_can_be_inserted_and_selected(test, conn)
    await pool.close()


class Psycopg3AsyncTestCase(NodeProvider, unittest.IsolatedAsyncioTestCase):

    def ensure_cratedb(self):
        if "CRATEDB_URI" in os.environ:
            crate_psql_url = os.environ["CRATEDB_URI"]
        else:
            (node, _) = self._new_node(self.CRATE_VERSION)
            node.start()
            psql_addr = node.addresses.psql
            crate_address = f'{psql_addr.host}:{psql_addr.port}'
            crate_psql_url = f'postgres://crate@{crate_address}/doc'
        return crate_psql_url

    def setUp(self):
        super().setUp()
        self.crate_psql_url = self.ensure_cratedb()

    async def test_basic_statements(self):
        await exec_queries_pooled(self, self.crate_psql_url)

    async def test_result_execute_client_cursor(self):
        await fetch_summits_client_cursor(self, self.crate_psql_url)

    async def test_result_execute_server_cursor(self):
        await fetch_summits_server_cursor(self, self.crate_psql_url)

    async def test_result_streaming(self):
        await fetch_summits_stream(self, self.crate_psql_url)

    def assertResultCommandEqual(self, result: psycopg.Cursor, command: str, msg=None):

        # Would be correct, but also would be a little strict, and mask the error message.
        # self.assertEqual(result.pgresult.status, psycopg.pq.ExecStatus.COMMAND_OK)

        # Satisfy mypy.
        assert result.pgresult

        self.assertEqual(result.pgresult.command_status, command.encode(), msg=msg)
