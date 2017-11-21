import asyncio
import asyncpg
import unittest
from crate.qa.tests import (
    NodeProvider
)


class AsyncpgTestCase(NodeProvider, unittest.TestCase):

    def test_basic_statements(self):
        node = self._new_node(self.CRATE_VERSION)
        node.start()

        async def run(hosts, future):
            pool = await asyncpg.create_pool(f'postgres://crate@{hosts}/doc')
            async with pool.acquire() as conn:
                create_result = await conn.execute("CREATE TABLE t1 (x int)")
                if int(create_result[7]) != 1:
                    future.set_exception(AssertionError("CREATE statement \
should've returned count 1 but the result was: " + create_result))

                insert_result = await conn.execute('INSERT INTO t1(x) VALUES(?)', 1)
                if int(insert_result[9]) != 1:
                    future.set_exception(AssertionError("INSERT statement \
should've returned count 1 but the result was: " + insert_result))

                refresh_result = await conn.execute('REFRESH TABLE t1')
                if int(refresh_result[8]) != 1:
                    future.set_exception(AssertionError("REFRESH statement \
should've returned count 1 but the result was: " + refresh_result))

                update_result = await conn.execute('UPDATE t1 SET x = ?', 2)
                if int(update_result[7]) != 1:
                    future.set_exception(AssertionError("UPDATE statement \
should've returned count 1 but the result was: " + update_result))

                refresh_result = await conn.execute('REFRESH TABLE t1')
                if int(refresh_result[8]) != 1:
                    future.set_exception(AssertionError("REFRESH statement \
should've returned count 1 but the result was: " + refresh_result))

                delete_result = await conn.execute('DELETE FROM t1 WHERE x = ?', 2)
                if int(delete_result[7]) != 1:
                    future.set_exception(AssertionError("DELETE statement \
should've returned count 1 but the result was: " + delete_result))

                if not future.done():
                    future.set_result("Success!")

                # Close the connection.
                await conn.close()

        loop = asyncio.get_event_loop()
        future = asyncio.Future()
        psql_protocol = node.addresses.psql
        crate_psql_url = str(psql_protocol.host) + ':' + \
            str(psql_protocol.port)
        asyncio.ensure_future(run(crate_psql_url, future))
        loop.run_until_complete(future)
        print(future.result())
