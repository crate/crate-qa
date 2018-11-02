import asyncio
import asyncpg
import unittest
from crate.qa.tests import NodeProvider


async def exec_queries(test, hosts):
    pool = await asyncpg.create_pool(f'postgres://crate@{hosts}/doc')
    async with pool.acquire() as conn:
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


class AsyncpgTestCase(NodeProvider, unittest.TestCase):

    def test_basic_statements(self):
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()
        loop = asyncio.get_event_loop()
        psql_addr = node.addresses.psql
        crate_psql_url = f'{psql_addr.host}:{psql_addr.port}'
        loop.run_until_complete(exec_queries(self, crate_psql_url))
