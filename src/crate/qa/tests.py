import os
import sys
import time
import math
import shutil
import operator
import tempfile
from pprint import pformat
from threading import Thread
from typing import NamedTuple
from functools import partial
from collections import OrderedDict
from distutils.version import StrictVersion as V
from faker.generator import random
from faker.providers import BaseProvider
from cr8.run_crate import CrateNode, get_crate
from cr8.insert_fake_data import DataFaker, generate_row, SELLECT_COLS
from cr8.insert_json import to_insert

DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
CRATEDB_0_57 = V('0.57.0')
EARTH_RADIUS = 6371  # earth radius in km


def fake_generator(columns):
    fake = ExtDataFaker()
    fakers = []
    for column_name, column_type in columns.items():
        fakers.append(fake.provider_for_column(column_name, column_type))
    return partial(generate_row, fakers)


def columns_for_table(conn, schema, table):
    c = conn.cursor()
    c.execute("SELECT min(version['number']) FROM sys.nodes")
    version = V(c.fetchone()[0])
    stmt = SELLECT_COLS.format(
        schema_column_name='table_schema' if version >= CRATEDB_0_57 else 'schema_name')
    c.execute(stmt, (schema, table, ))
    return OrderedDict(c.fetchall())


def insert_data(conn, schema, table, num_rows):
    cols = columns_for_table(conn, schema, table)
    stmt, args = to_insert(f'"{schema}"."{table}"', cols)
    row = fake_generator(cols)
    c = conn.cursor()
    c.executemany(stmt, [row() for x in range(num_rows)])
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
        print('-' * 79)
        print(f'waited: {waited} last duration: {duration} timeout: {timeout}')
        cursor = conn.cursor()
        cursor.execute('SELECT count(*), table_name, state FROM sys.shards GROUP BY 2, 3 ORDER BY 2')
        rs = cursor.fetchall()
        print(f'=== {rs}')
        print('-' * 79)
    raise TimeoutError(f"Shards didn't become active within {timeout}s.")


def _dest_point(point, distance, bearing, radius):
    # calculation taken from
    # https://cdn.rawgit.com/chrisveness/geodesy/v1.1.2/latlon-spherical.js
    # https://www.movable-type.co.uk/scripts/latlong.html

    δ = distance / radius  # angular distance in rad
    θ = math.radians(bearing)

    φ1 = math.radians(point[1])
    λ1 = math.radians(point[0])

    sinφ1 = math.sin(φ1)
    cosφ1 = math.cos(φ1)
    sinδ = math.sin(δ)
    cosδ = math.cos(δ)
    sinθ = math.sin(θ)
    cosθ = math.cos(θ)

    sinφ2 = sinφ1 * cosδ + cosφ1 * sinδ * cosθ
    φ2 = math.asin(sinφ2)
    y = sinθ * sinδ * cosφ1
    x = cosδ - sinφ1 * sinφ2
    λ2 = λ1 + math.atan2(y, x)

    return [
        (math.degrees(λ2) + 540) % 360 - 180,  # normalise to −180..+180°
        math.degrees(φ2)
    ]


class GeoShapeProvider(BaseProvider):
    """
    This class can be removed once the GeoSpatialProvider of the cr8 package
    provide the geo_shape() method.
    """

    def geo_shape(self, sides=5, center=None, distance=None):
        """
        Return a WKT string for a POLYGON with given amount of sides.
        The polygon is defined by its center (random point if not provided) and
        the distance (random distance if not provided; in km) of the points to
        its center.
        """
        assert isinstance(sides, int)

        if distance is None:
            distance = self.random_int(100, 1000)
        else:
            # 6371 => earth radius in km
            # assert that shape radius is maximum half of earth's circumference
            assert isinstance(distance, int)
            assert distance <= EARTH_RADIUS * math.pi, \
                'distance must not be greater than half of earth\'s circumference'

        if center is None:
            u = self.generator.random.uniform
            # required minimal spherical distance from north/southpole
            dp = distance * 180.0 / EARTH_RADIUS / math.pi
            center = [
                u(-180.0, 180.0),
                u(-90.0 + dp, 90.0 - dp)
            ]
        else:
            assert -180.0 <= center[0] <= 180.0, 'Longitude out of bounds'
            assert -90.0 <= center[1] <= 90.0, 'Latitude out of bounds'

        angles = list(self.random_sample_unique(range(360), sides))
        angles.sort()
        points = [_dest_point(center, distance, bearing, EARTH_RADIUS) for bearing in angles]
        # close polygon
        points.append(points[0])

        path = ', '.join([' '.join(p) for p in ([str(lon), str(lat)] for lon, lat in points)])
        return f'POLYGON (( {path} ))'


class ExtDataFaker(DataFaker):
    """
    This class can be removed once the GeoSpatialProvider of the cr8 package
    provide the geo_shape() method.
    """

    def __init__(self):
        super().__init__()
        self.fake.add_provider(GeoShapeProvider)
        self._type_default.update({
            'geo_shape': operator.attrgetter('geo_shape'),
            'byte': lambda f: partial(f.random_int, min=-128, max=127),
        })


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
