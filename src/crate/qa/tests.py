import os
import sys
import time
import signal
import shutil
import string
import tempfile
import functools
from pprint import pformat
from threading import Thread
from typing import Dict, Any, NamedTuple, Iterable, List, Optional, Tuple

from faker.generator import random
from glob import glob
from cr8.run_crate import CrateNode, get_crate, _extract_version, parse_version
from cr8.insert_fake_data import SELLECT_COLS, Column, create_row_generator
from cr8.insert_json import to_insert

DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

CRATEDB_0_57 = (0, 57, 0)


print_error = functools.partial(print, file=sys.stderr)


JDK_8_JAVA_HOME_CANDIDATES = (
    '/usr/lib/jvm/java-8-openjdk',
    '/usr/lib/java-1.8.0',
) + tuple(glob('/Library/Java/JavaVirtualMachines/jdk*1.8*/Contents/Home'))


class UpgradePath(NamedTuple):
    from_version: str
    to_version: str

    def __repr__(self):
        return f'{self.from_version} -> {self.to_version}'


def prepare_env(java_home_candidates: Iterable[str]) -> dict:
    for candidate in filter(os.path.exists, java_home_candidates):
        return {'JAVA_HOME': candidate}
    return {}


def gen_id() -> str:
    return ''.join([random.choice(string.hexdigits) for x in range(12)])


def test_settings(version: Tuple[int, int, int]) -> Dict[str, Any]:
    s = {
        'cluster.routing.allocation.disk.watermark.low': '1024k',
        'cluster.routing.allocation.disk.watermark.high': '512k',
    }
    if version >= (3, 0, 0):
        s.update({
            'cluster.routing.allocation.disk.watermark.flood_stage': '256k',
        })
    return s


def remove_unsupported_settings(version: Tuple[int, int, int], settings: dict) -> Dict[str, Any]:
    new_settings = dict(settings)
    if version >= (4, 0, 0):
        new_settings.pop('license.enterprise', None)
    else:
        new_settings.pop("discovery.seed_hosts", None)
        new_settings.pop("cluster.initial_master_nodes", None)

    return new_settings


def columns_for_table(conn, schema, table):
    c = conn.cursor()
    c.execute("SELECT min(version['number']) FROM sys.nodes")
    version = parse_version(c.fetchone()[0])
    stmt = SELLECT_COLS.format(
        schema_column_name='table_schema' if version >= CRATEDB_0_57 else 'schema_name')
    c.execute(stmt, (schema, table, ))
    return [Column(*row) for row in c.fetchall()]


def insert_data(conn, schema, table, num_rows):
    cols = columns_for_table(conn, schema, table)
    columns_dict = {r.name: r.type_name for r in cols}
    stmt, args = to_insert(f'"{schema}"."{table}"', columns_dict)
    gen_row = create_row_generator(cols)
    c = conn.cursor()
    c.executemany(stmt, [gen_row() for x in range(num_rows)])
    c.execute(f'REFRESH TABLE "{schema}"."{table}"')


def wait_for_active_shards(cursor, num_active=0, timeout=60, f=1.2):
    """Wait for shards to become active

    If `num_active` is `0` this will wait until there are no shards that aren't
    started.
    If `num_active > 0` this will wait until there are `num_active` shards with
    the state `STARTED`
    """
    waited = 0
    duration = 0.1
    while waited < timeout:
        if num_active > 0:
            cursor.execute(
                "SELECT count(*) FROM sys.shards where state = 'STARTED'")
            if int(cursor.fetchone()[0]) == num_active:
                return
        else:
            cursor.execute(
                "SELECT count(*) FROM sys.shards WHERE state != 'STARTED'")
            if int(cursor.fetchone()[0]) == 0:
                return
        time.sleep(duration)
        waited += duration
        duration *= f

    if DEBUG:
        print('-' * 70)
        print(f'waited: {waited} last duration: {duration} timeout: {timeout}')
        cursor.execute('SELECT count(*), table_name, state FROM sys.shards GROUP BY 2, 3 ORDER BY 2')
        rs = cursor.fetchall()
        print(f'=== {rs}')
        print('-' * 70)
    raise TimeoutError(f"Shards {num_active} didn't become active within {timeout}s.")


class VersionDef(NamedTuple):
    version: str
    java_home: Iterable[str]


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

    def stop(self):
        for node in self._nodes:
            node.stop()

    def node(self):
        return random.choice(self._nodes)

    def nodes(self):
        return self._nodes

    def __next__(self):
        return next(self._nodes)

    def __setitem__(self, idx, node):
        self._nodes[idx] = node

    def __getitem__(self, idx):
        return self._nodes[idx]


class NodeProvider:

    CRATE_VERSION = os.environ.get('CRATE_VERSION', 'latest-nightly')
    CRATE_HEAP_SIZE = os.environ.get('CRATE_HEAP_SIZE', '768m')
    DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

    def __init__(self, *args, **kwargs):
        self.tmpdirs = []
        super().__init__(*args, **kwargs)

    def mkdtemp(self, *args):
        tmp = tempfile.mkdtemp()
        self.tmpdirs.append(tmp)
        return os.path.join(tmp, *args)

    def _new_cluster(self,
                     version,
                     num_nodes: int,
                     data_paths: Optional[List[str]] = None,
                     settings: Optional[Dict[str, str]] = None,
                     env=None,
                     explicit_discovery=True) -> CrateCluster:
        """ data_paths has 'num_nodes' elements and data_paths[i] stores path of the i-th node. 'None' if called first time."""
        assert hasattr(self, '_new_node'), "NodeProvider must have _new_node method"
        settings = settings or {}
        for port in ['transport.tcp.port', 'http.port', 'psql.port']:
            assert port not in settings, f"Must not define {port} in settings"
        cluster_name = gen_id()
        s = {
            'cluster.name': cluster_name,
            'gateway.recover_after_nodes': num_nodes,
            'gateway.expected_nodes': num_nodes,
        }
        if explicit_discovery:
            s["discovery.seed_hosts"] = ",".join(f"127.0.0.1:{4300 + x}" for x in range(num_nodes))
            s["cluster.initial_master_nodes"] = ",".join(f"{cluster_name}-{x}" for x in range(num_nodes))
        s.update(settings)
        nodes = []
        for id in range(num_nodes):
            node_settings = s.copy()
            node_settings['node.name'] = cluster_name + '-' + str(id)
            if explicit_discovery:
                node_settings["transport.tcp.port"] = 4300 + id
            """ We want to preserve data_paths when we start cluster second time during a test.
            Path is taken from the first start.
            """
            if data_paths is not None:
                node_settings['path.data'] = data_paths[id]
            nodes.append(self._new_node(version, node_settings, env)[0])
        return CrateCluster(nodes)

    def _new_heterogeneous_cluster(self, versions, settings=None):
        self.assertTrue(hasattr(self, '_new_node'))
        settings = settings or {}
        for port in ['transport.tcp.port', 'http.port', 'psql.port']:
            self.assertNotIn(port, settings)
        num_nodes = len(versions)
        s = {
            'cluster.name': gen_id(),
            'gateway.recover_after_nodes': num_nodes,
            'gateway.expected_nodes': num_nodes
        }
        s.update(settings)
        nodes = []
        for i, version in enumerate(versions):
            s['node.name'] = f"{s['cluster.name']}-{i}"
            nodes.append(self._new_node(version, s)[0])
        return CrateCluster(nodes)

    def upgrade_node(self, old_node, new_version):
        old_node.stop()
        self._on_stop.remove(old_node)
        (new_node, _) = self._new_node(new_version, settings=old_node._settings)
        new_node.start()
        return new_node

    def setUp(self):
        self._on_stop = []
        self._log_consumers = []

        def new_node(version, settings=None, env=None):
            crate_dir = get_crate(version)
            version_tuple = _extract_version(crate_dir)
            s = {
                'cluster.name': 'crate-qa',
            }
            s.update(settings or {})
            s.update(test_settings(version_tuple))

            """ After removal of the node.max_local_storage_nodes in 5.0, every node has it's own path.data generated on node creation.
            However, we don't want to re-generate data path if we create a node based on existing settings, for example
            upgrade_node calls this method with old_node._settings
            """
            if "path.data" not in s:
                s['path.data'] = self.mkdtemp()
            if "path.logs" not in s:
                s["path.logs"] = self.mkdtemp()

            s = remove_unsupported_settings(version_tuple, s)
            e = {
                'CRATE_HEAP_SIZE': self.CRATE_HEAP_SIZE,
                'CRATE_DISABLE_GC_LOGGING': '1',
                'CRATE_HOME': crate_dir,
            }
            e.update(env or {})

            if self.DEBUG:
                print(f'# Running CrateDB {version} ({version_tuple}) ...')
                s_nice = pformat(s)
                print(f'with settings: {s_nice}')
                e_nice = pformat(e)
                print(f'with environment: {e_nice}')

            n = CrateNode(
                crate_dir=crate_dir,
                keep_data=True,
                settings=s,
                env=e,
            )
            n._settings = s  # CrateNode does not hold its settings
            self._add_log_consumer(n)
            self._on_stop.append(n)
            return (n, version_tuple)
        self._new_node = new_node

    def tearDown(self):
        self._crate_logs_on_failure()
        self._process_on_stop()
        for tmp in self.tmpdirs:
            if DEBUG:
                print(f'# Removing temporary directory {tmp}')
            shutil.rmtree(tmp, ignore_errors=True)
        self.tmpdirs.clear()

    def _process_on_stop(self):
        for n in self._on_stop:
            n.stop()
        self._on_stop.clear()

    def _add_log_consumer(self, node: CrateNode):
        lines: List[str] = []
        node.monitor.consumers.append(lines.append)
        self._log_consumers.append((node, lines))

    def _crate_logs_on_failure(self):
        for node, lines in self._log_consumers:
            node.monitor.consumers.remove(lines.append)
            if self._has_error():
                print_error('=' * 70)
                print_error('CrateDB logs for test ' + self.id())
                print_error('-' * 70)
                for line in lines:
                    print_error(line)
                print_error('-' * 70)
        self._log_consumers.clear()

    def _has_error(self) -> bool:
        # _outcome is set if NodeProvider is mixed with TestCase
        outcome = getattr(self, "_outcome", None)
        if not outcome:
            return False
        if hasattr(outcome, "success"):
            return not outcome.success
        return any(error for (_, error) in outcome.errors)  # type: ignore


def assert_busy(assertion, timeout=120, f=2.0):
    waited = 0
    sleep_interval_sec = 0.1
    assertion_error = None
    while waited < timeout:
        try:
            assertion()
            return
        except AssertionError as e:
            assertion_error = e
            time.sleep(sleep_interval_sec)
            waited += sleep_interval_sec
            sleep_interval_sec *= f
    raise assertion_error


class FunctionTimeoutError(Exception):
    pass


def timeout(seconds=10, error_message="timed out!"):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise FunctionTimeoutError(error_message)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wrapper

    return decorator
