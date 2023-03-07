"""
About
=====
Test cases for CrateDB using `asyncpg`.

Usage
=====
Normally, this will be executed through the main Python test suite
triggered through the toplevel `Jenkinsfile`.

However, to run the `asyncpg` tests only, for example on a CrateDB
instance already provided through Docker, there's an alternative
option which goes along like this::

    # Run CrateDB.
    docker run -it --rm --publish=5432:5432 crate/crate:nightly

    # Run test suite.
    export CRATEDB_URI=postgres://crate@localhost:5432/doc
    python -m unittest discover -vvvf -s tests/client_tests -k test_asyncpg

"""
import os
import asyncio
import asyncpg
import unittest
from crate.qa.tests import NodeProvider


async def basic_queries(test, conn):

    await conn.execute("DROP TABLE IF EXISTS t1")

    result = await conn.execute(
        "CREATE TABLE t1 (x int primary key, y int)")
    test.assertEqual(result, 'CREATE 1')

    result = await conn.execute('INSERT INTO t1 (x) VALUES (?)', 1)
    test.assertEqual(result, 'INSERT 0 1')

    result = await conn.execute('REFRESH TABLE t1')
    test.assertEqual(result, 'REFRESH 1')

    result = await conn.execute('UPDATE t1 SET y = ?', 2)
    test.assertEqual(result, 'UPDATE 1')

    result = await conn.execute('REFRESH TABLE t1')
    test.assertEqual(result, 'REFRESH 1')

    result = await conn.execute('DELETE FROM t1 WHERE y = ?', 2)
    test.assertEqual(result, 'DELETE 1')

    result = await conn.execute('''
        INSERT INTO t1 (x) (
            SELECT unnest FROM unnest([1, 2]) WHERE unnest = ?
        )
    ''', 1)
    test.assertEqual(result, 'INSERT 0 1')

    result = await conn.execute('''
        INSERT INTO t1 (x) (
            SELECT unnest FROM unnest([1, 2]) WHERE unnest = ?)
        ON CONFLICT (x) DO UPDATE SET y = ?
    ''', 1, 2)
    test.assertEqual(result, 'INSERT 0 1')


async def record_type_can_be_read_using_binary_streaming(test, conn):
    result = await conn.fetch('SELECT pg_catalog.pg_get_keywords()')
    keyword = sorted([row[0] for row in result], key=lambda x: x[0])[0]
    test.assertEqual(keyword, ('absolute', 'U', 'unreserved'))


async def bitstring_can_be_inserted_and_selected_using_binary_encoding(test, conn):
    xs = asyncpg.BitString('0101')
    await conn.execute('drop table if exists tbl_bit')
    await conn.execute('create table tbl_bit (xs bit(4))')
    await conn.execute('insert into tbl_bit (xs) values (?)', xs)
    await conn.execute('refresh table tbl_bit')
    result = await conn.fetch('select xs from tbl_bit')
    test.assertEqual(result[0][0], xs)


async def fetch_summits(test, uri):
    conn = await asyncpg.connect(uri)
    async with conn.transaction():
        cur = await conn.cursor(
            'select mountain from sys.summits order by height desc')
        first, second = await cur.fetch(2)
        third, fourth = await cur.fetch(2)
    test.assertEqual(first['mountain'], 'Mont Blanc')
    test.assertEqual(second['mountain'], 'Monte Rosa')
    test.assertEqual(third['mountain'], 'Dom')
    test.assertEqual(fourth['mountain'], 'Liskamm')


async def exec_queries_pooled(test, uri):
    pool = await asyncpg.create_pool(uri)
    async with pool.acquire() as conn:
        await basic_queries(test, conn)
        await record_type_can_be_read_using_binary_streaming(test, conn)
        await bitstring_can_be_inserted_and_selected_using_binary_encoding(test, conn)


class AsyncpgTestCase(NodeProvider, unittest.TestCase):

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

    def test_basic_statements(self):
        crate_psql_url = self.ensure_cratedb()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(exec_queries_pooled(self, crate_psql_url))

    def test_result_streaming_using_fetch_size(self):
        crate_psql_url = self.ensure_cratedb()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            fetch_summits(self, crate_psql_url))
