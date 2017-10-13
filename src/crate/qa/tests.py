import os
import time
import shutil
import random
import tempfile
from pprint import pformat
from threading import Thread
from typing import NamedTuple

from cr8.run_crate import CrateNode, get_crate


def wait_for_active_shards(cursor):
    """Wait until all shards are started"""
    waited = 0
    duration = 0.01
    while waited < 20:
        cursor.execute(
            "SELECT count(*) FROM sys.shards WHERE state != 'STARTED'")
        if int(cursor.fetchone()[0]) == 0:
            return
        time.sleep(duration)
        waited += duration
        duration *= 2
    raise TimeoutError("Shards didn't become active in time")


class VersionDef(NamedTuple):
    version: str
    upgrade_segments: bool


class CrateCluster:

    def __init__(self, nodes=[]):
        self._nodes = nodes

    def start(self):
        threads = []
        for node in self._nodes:
            t = Thread(target=node.start)
            t.start()
            threads.append(t)
        [t.join() for t in threads]

    def node(self):
        return random.choice(self._nodes)


class NodeProvider:

    CRATE_VERSION = os.environ.get('CRATE_VERSION', 'latest-nightly')
    CRATE_HEAP_SIZE = os.environ.get('CRATE_HEAP_SIZE', '512m')
    DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

    def __init__(self, *args, **kwargs):
        self.tmpdirs = []
        super().__init__(*args, **kwargs)

    def mkdtemp(self, *args):
        tmp = tempfile.mkdtemp()
        self.tmpdirs.append(tmp)
        return os.path.join(tmp, *args)

    def _unicast_hosts(self, num, transport_port=4300):
        return ','.join([
            '127.0.0.1:' + str(transport_port + x)
            for x in range(num)
        ])

    def _new_cluster(self, version, num_nodes, settings={}):
        self.assertTrue(hasattr(self, '_new_node'))
        for port in ['transport.tcp.port', 'http.port', 'psql.port']:
            self.assertFalse(port in settings)
        s = {
            'cluster.name': 'crate-qa-cluster',
            'discovery.zen.ping.unicast.hosts': self._unicast_hosts(num_nodes),
            'discovery.zen.minimum_master_nodes': str(int(num_nodes / 2.0 + 1)),
            'gateway.recover_after_nodes': str(num_nodes),
            'gateway.expected_nodes': str(num_nodes),
            'node.max_local_storage_nodes': str(num_nodes),
        }
        s.update(settings)
        nodes = []
        for id in range(num_nodes):
            nodes.append(self._new_node(version, s))
        return CrateCluster(nodes)

    def setUp(self):
        self._path_data = self.mkdtemp()
        self._on_stop = []

        def new_node(version, settings={}):
            s = {
                'path.data': self._path_data,
                'cluster.name': 'crate-qa'
            }
            s.update(settings)
            e = {
                'CRATE_HEAP_SIZE': self.CRATE_HEAP_SIZE,
            }

            print(f'# Running CrateDB {version} ...')
            if self.DEBUG:
                s_nice = pformat(s)
                print(f'with settings: {s_nice}')
                e_nice = pformat(e)
                print(f'with environment: {e_nice}')

            n = CrateNode(
                crate_dir=get_crate(version),
                keep_data=True,
                settings=s,
                env=e,
            )
            self._on_stop.append(n.stop)
            return n
        self._new_node = new_node

    def tearDown(self):
        for tmp in self.tmpdirs:
            print(f'# Removing temporary directory {tmp}')
            shutil.rmtree(tmp, ignore_errors=True)
        self.tmpdirs.clear()
        self._process_on_stop()

    def _process_on_stop(self):
        for to_stop in self._on_stop:
            to_stop()
        self._on_stop.clear()
