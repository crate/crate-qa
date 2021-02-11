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
            SELECT col1 FROM unnest([1, 2]) WHERE col1 = ?
        )
    ''', 1)
    test.assertEqual(result, 'INSERT 0 1')

    result = await conn.execute('''
        INSERT INTO t1 (x) (
            SELECT col1 FROM unnest([1, 2]) WHERE col1 = ?)
        ON CONFLICT (x) DO UPDATE SET y = ?
    ''', 1, 2)
    test.assertEqual(result, 'INSERT 0 1')


async def record_type_can_be_read_using_binary_streaming(test, conn):
    result = await conn.fetch('SELECT pg_catalog.pg_get_keywords()')
    keyword = sorted([row[0] for row in result], key=lambda x: x[0])[0]
    test.assertEqual(keyword, ('add', 'R', 'reserved'))


async def fetch_summits(test, host, port):
    conn = await asyncpg.connect(
        host=host, port=port, user='crate', database='doc')
    async with conn.transaction():
        cur = await conn.cursor(
            'select mountain from sys.summits order by height desc')
        first, second = await cur.fetch(2)
        third, fourth = await cur.fetch(2)
    test.assertEqual(first['mountain'], 'Mont Blanc')
    test.assertEqual(second['mountain'], 'Monte Rosa')
    test.assertEqual(third['mountain'], 'Dom')
    test.assertEqual(fourth['mountain'], 'Liskamm')


async def exec_queries_pooled(test, hosts):
    pool = await asyncpg.create_pool(f'postgres://crate@{hosts}/doc')
    async with pool.acquire() as conn:
        await basic_queries(test, conn)
        await record_type_can_be_read_using_binary_streaming(test, conn)


class AsyncpgTestCase(NodeProvider, unittest.TestCase):

    def test_basic_statements(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()
        loop = asyncio.get_event_loop()
        psql_addr = node.addresses.psql
        crate_psql_url = f'{psql_addr.host}:{psql_addr.port}'
        loop.run_until_complete(exec_queries_pooled(self, crate_psql_url))

    def test_result_streaming_using_fetch_size(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()
        loop = asyncio.get_event_loop()
        psql_addr = node.addresses.psql
        loop.run_until_complete(
            fetch_summits(self, psql_addr.host, psql_addr.port))
