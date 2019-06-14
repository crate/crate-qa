import asyncio
import aiopg
import unittest
from crate.qa.tests import NodeProvider


async def exec_queries(test, hosts):
    pool = await aiopg.create_pool(f'postgres://crate@{hosts}/doc')
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "CREATE TABLE t1 (x int primary key, y int)")
            test.assertEqual(conn.status, 1)

            await cursor.execute('INSERT INTO t1 (x) VALUES (%s)', (1,))
            test.assertEqual(conn.status, 1)

            await cursor.execute('REFRESH TABLE t1')
            test.assertEqual(conn.status, 1)

            await cursor.execute('UPDATE t1 SET y = %s', (2,))
            test.assertEqual(conn.status, 1)

            await cursor.execute('REFRESH TABLE t1')
            test.assertEqual(conn.status, 1)

            await cursor.execute('DELETE FROM t1 WHERE y = %s', (2,))
            test.assertEqual(conn.status, 1)

            await cursor.execute('''
                INSERT INTO t1 (x) (
                    SELECT col1 FROM unnest([1, 2]) WHERE col1 = %s
                )
            ''', (1,))
            test.assertEqual(conn.status, 1)

            await cursor.execute('''
                INSERT INTO t1 (x) (
                    SELECT col1 FROM unnest([1, 2]) WHERE col1 = %s)
                ON CONFLICT (x) DO UPDATE SET y = %s
            ''', (1, 2,))
            test.assertEqual(conn.status, 1)


async def fetch_summits(test, host, port):
    async with aiopg.connect(
        host=host, port=port, user='crate', database='doc'
    ) as conn:
        async with conn.cursor() as cur:
            async with cur.begin():
                await cur.execute(
                    'select mountain from sys.summits order by height desc')
                first, second = await cur.fetchmany(2)
                third, fourth = await cur.fetchmany(2)
                test.assertEqual(first[0], 'Mont Blanc')
                test.assertEqual(second[0], 'Monte Rosa')
                test.assertEqual(third[0], 'Dom')
                test.assertEqual(fourth[0], 'Liskamm')


class AiopgTestCase(NodeProvider, unittest.TestCase):

    def test_basic_statements(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()
        loop = asyncio.get_event_loop()
        psql_addr = node.addresses.psql
        crate_psql_url = f'{psql_addr.host}:{psql_addr.port}'
        loop.run_until_complete(exec_queries(self, crate_psql_url))

    def test_result_streaming_using_fetch_size(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()
        loop = asyncio.get_event_loop()
        psql_addr = node.addresses.psql
        loop.run_until_complete(
            fetch_summits(self, psql_addr.host, psql_addr.port))
