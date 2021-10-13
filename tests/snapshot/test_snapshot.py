import random
import shutil
import threading
import unittest

from cr8.run_crate import wait_until
from crate.client import connect
from crate.client.exceptions import ProgrammingError
from crate.qa.minio_svr import MinioServer, _is_up
from crate.qa.tests import NodeProvider, insert_data, wait_for_active_shards, gen_id, assert_busy


class SnapshotOperationTest(NodeProvider, unittest.TestCase):

    PATH_DATA = 'data_test_snapshot_ops'

    def tearDown(self):
        shutil.rmtree(self.PATH_DATA, ignore_errors=True)

    def _assert_num_docs(self, conn, expected_count):
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM doc.test')
        rowcount = c.fetchone()[0]
        self.assertEqual(rowcount, expected_count)

    def test_snapshot_restore_and_drop_in_parallel(self):
        """Test to run the drop and restore operation on two different
           snapshots in parallel.

        The purpose of this test is to validate that the snapshot mechanism
        of CrateDB can handle the two operations in parallel. Here, Minio is
        used as s3 backend for the repository, but this should work on any
        other backend as well.
        """
        with MinioServer() as minio:
            t = threading.Thread(target=minio.run)
            t.daemon = True
            t.start()
            wait_until(lambda: _is_up('127.0.0.1', 9000))

            num_nodes = random.randint(3, 5)
            number_of_shards = random.randint(1, 3)
            number_of_replicas = random.randint(0, 2)
            num_docs = random.randint(1, 100)

            cluster_settings = {
                'cluster.name': gen_id(),
                'path.data': self.PATH_DATA
            }
            shutil.rmtree(self.PATH_DATA, ignore_errors=True)
            cluster = self._new_cluster('latest-nightly', num_nodes, settings=cluster_settings)
            cluster.start()

            with connect(cluster.node().http_url, error_trace=True) as conn:
                c = conn.cursor()
                wait_for_active_shards(c)
                c.execute('''
                            create table doc.test(x int) clustered into ? shards with( number_of_replicas =?)
                         ''', (number_of_shards, number_of_replicas,))

                insert_data(conn, 'doc', 'test', num_docs)

                c.execute('''
                            CREATE REPOSITORY repo TYPE S3
                            WITH (access_key = 'minio',
                            secret_key = 'miniostorage',
                            bucket='backups',
                            endpoint = '127.0.0.1:9000',
                            protocol = 'http')
                        ''')

                c.execute('CREATE SNAPSHOT repo.snapshot1 TABLE doc.test WITH (wait_for_completion = true)')
                c.execute('CREATE SNAPSHOT repo.snapshot2 TABLE doc.test WITH (wait_for_completion = true)')
                c.execute('DROP TABLE doc.test')
                # Drop snapshot2 while the restore of snapshot1 is still running
                c.execute('RESTORE SNAPSHOT repo.snapshot1 ALL WITH (wait_for_completion = false)')
                try:
                    c.execute('DROP SNAPSHOT repo.snapshot2')
                except ProgrammingError:
                    self.fail("Restore and Drop Snapshot operation should work in parallel")

                assert_busy(lambda: self._assert_num_docs(conn, num_docs))

            cluster.stop()
