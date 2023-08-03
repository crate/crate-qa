import unittest
import random
from threading import Thread, Lock, Event
from time import sleep
from crate.client import connect
from crate.qa.tests import NodeProvider, wait_for_active_shards, UpgradePath, insert_data

ROLLING_UPGRADES = (
    UpgradePath('5.2.x', '5.3.x'),
    UpgradePath('5.3.x', '5.4.x'),
    UpgradePath('5.4.x', 'latest-nightly')
)


class IngestRollingUpgradeTest(NodeProvider, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.ingested_rows = 0
        self.lock = Lock()
        self.stop_ingest_event = Event()

    def tearDown(self):
        super().tearDown()
        self.stop_ingest_event.set()

    def test_ingest_rolling_upgrade(self):
        for path in ROLLING_UPGRADES:
            print(path)
            with self.subTest(repr(path)):
                try:
                    self.setUp()
                    self._test_ingest_rolling_upgrade(path)
                finally:
                    self.tearDown()

    def _test_ingest_rolling_upgrade(self, path):

        shards, replicas = (random.randint(3, 5), 1)
        expected_active_shards = shards + shards * replicas

        settings = {"transport.netty.worker_count": 16}
        cluster = self._new_cluster(path.from_version, 3, settings=settings)
        cluster.start()

        with connect(cluster.node().http_url, error_trace=True) as conn:
            c = conn.cursor()
            c.execute(
                f'''CREATE TABLE doc.t1 (x string) CLUSTERED INTO {shards} SHARDS WITH (number_of_replicas={replicas})'''
            )
            self.insert_data(conn)

        ingest_node = cluster.nodes()[0]
        thread = Thread(name="ingest", target=self.ingest, args=(ingest_node, self.stop_ingest_event), daemon=True)
        thread.start()

        remaining_nodes = cluster.nodes()[1:]
        for idx, node in enumerate(remaining_nodes):
            new_node = self.upgrade_node(node, path.to_version)
            cluster[idx] = new_node
            with connect(new_node.http_url, error_trace=True) as conn:
                c = conn.cursor()
                wait_for_active_shards(c, expected_active_shards)
                self.lock.acquire()
                c.execute("select count(_id) from doc.t1")
                res = c.fetchall()
                self.assertEqual(self.ingested_rows, res[0][0])
                self.lock.release()

    def ingest(self, node, stop_ingest_event):
        while True:
            if stop_ingest_event.is_set():
                return
            try:
                with connect(node.http_url, error_trace=True) as conn:
                    self.lock.acquire()
                    self.insert_data(conn)
                    self.lock.release()
                    sleep(0.1)
            except Exception as e:
                print(f'Error while ingesting {e}')

    def insert_data(self, conn):
        number_to_ingest = random.randint(1000, 5000)
        insert_data(conn, 'doc', 't1', number_to_ingest)
        self.ingested_rows += number_to_ingest
