import os
import re
import socket
import unittest
from pathlib import Path
from crate.client import connect
from crate.client.exceptions import ProgrammingError
from crate.qa.tests import NodeProvider
from faker import Faker
from faker.config import AVAILABLE_LOCALES
from faker.generator import random


def randbool():
    return True if random.getrandbits(1) else False


class StartupTest(NodeProvider, unittest.TestCase):
    fake = Faker(random.choice(list(AVAILABLE_LOCALES)))

    def assert_mqtt_port(self, predicate):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            result = s.connect_ex(('127.0.0.1', 1883))
            self.assertTrue(predicate(result))

    def test_name_settings(self):
        settings = {
            'node.name': self.fake.name(),
            'cluster.name': self.fake.bothify(text='????.##'),
        }
        (node, _) = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT name FROM sys.cluster
            ''')
            res = cur.fetchone()
            self.assertEqual(res[0], settings['cluster.name'])

            cur.execute('''
                SELECT name from sys.nodes
            ''')
            res = cur.fetchone()
            self.assertEqual(res[0], settings['node.name'])

    def test_path_settings(self):
        settings = {
            'path.data': self.mkdtemp(),
            'path.logs': self.mkdtemp(),
            'cluster.name': 'crate',
        }
        (node, _) = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()
        with connect(node.http_url, error_trace=True) as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT fs['data']['path'] FROM sys.nodes
            ''')
            res = cur.fetchone()
            self.assertTrue(res[0][0].startswith(node.data_path))
        self.assertTrue(os.path.exists(
            os.path.join(settings['path.logs'],
                         settings['cluster.name'] + '.log')
        ))

    def _assert_enterprise_equal(self, node, is_enterprise):
        with connect(node.http_url, error_trace=True) as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT settings['license']['enterprise'] AS enabled,
                       settings['license']['ident'] AS ident
                FROM sys.cluster
            ''')
            res = cur.fetchone()
            self.assertEqual(res[0], is_enterprise)
            self.assertEqual(res[1], '')

    def test_enterprise_enabled(self):
        settings = dict({
            'license.enterprise': True,
            'lang.js.enabled': randbool(),
            'ingestion.mqtt.enabled': randbool(),
            'auth.host_based.enabled': randbool(),
            'auth.host_based.config.0.user': 'crate',
            'auth.host_based.config.0.host': '127.0.0.1',
            'auth.host_based.config.0.protocol': 'http',
        })

        (node, _) = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()

        self._assert_enterprise_equal(node, True)

        with connect(node.http_url, error_trace=True) as conn:
            cur = conn.cursor()
            # User Management
            cur.execute('''
                SELECT name, superuser FROM sys.users
            ''')
            res = cur.fetchone()
            self.assertEqual(res[0], 'crate')
            self.assertEqual(res[1], True)

            # UDF Javascript
            if settings['lang.js.enabled']:
                cur.execute('''
                    CREATE FUNCTION js_add(LONG, LONG) RETURNS LONG
                    LANGUAGE javascript
                    AS 'function js_add(a, b) { return a + b; }'
                ''')
                cur.execute('''
                    SELECT routine_name, routine_body
                    FROM information_schema.routines
                    WHERE routine_type = 'FUNCTION'
                ''')
                res = cur.fetchone()
                self.assertEqual(res[0], 'js_add')
                self.assertEqual(res[1], 'javascript')

            # MQTT
            if settings['ingestion.mqtt.enabled']:
                self.assert_mqtt_port(lambda x: x == 0)

    def test_enterprise_disabled(self):
        settings = dict({
            'license.enterprise': False,
        })

        (node, _) = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()

        self._assert_enterprise_equal(node, False)

        with connect(node.http_url, error_trace=True) as conn:
            cur = conn.cursor()
            # User Management
            with self.assertRaisesRegex(ProgrammingError,
                                        'Relation \'sys.users\' unknown'):
                cur.execute('''
                    SELECT name, superuser
                    FROM sys.users
                ''')
            # UDF Javascript
            with self.assertRaisesRegex(ProgrammingError,
                                        '\'javascript\' is not a valid UDF language'):
                cur.execute('''
                    CREATE FUNCTION js_add(LONG, LONG) RETURNS LONG
                    LANGUAGE javascript
                    AS 'function js_add(a, b) { return a + b; }'
                ''')
            # MQTT
            self.assert_mqtt_port(lambda x: x > 0)

    def test_startup_logs(self):

        # Create CRATE_HOME directory
        tmp_home = self.mkdtemp()
        Path(tmp_home, 'config').mkdir()

        node_name = self.fake.last_name()
        settings = dict({
            'node.name': node_name,
            'path.home': tmp_home,
        })

        log_file_path = Path(tmp_home, 'crate.log')
        self.create_log_from_template(log_file_path, tmp_home)

        (node, version_tuple) = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()

        # Check entries in log file
        def verify_and_extract_content_from_log_line(log_line):
            self.assertTrue(' [' + node_name + '] ' in log_line, 'line does not contain correct node name')
            if '[o.e.n.Node               ]' in line:
                return log_line.split(' [' + node_name + '] ')[1]
            return None

        with open(log_file_path, 'r') as f:
            for lineIdx, line in enumerate(f):
                line_ctx = verify_and_extract_content_from_log_line(line)
                if line_ctx:
                    if lineIdx == 1:
                        self.assertTrue('initializing', line_ctx)
                    elif lineIdx == 2:
                        self.assertTrue(re.match(r'node name \[' + node_name + '\], node ID \[.+\]', line_ctx))
                    elif lineIdx == 3:
                        version_str = '.'.join([str(v) for v in version_tuple])
                        self.assertTrue(re.match(r'CrateDB version\[' + version_str + '-SNAPSHOT\], ' +
                                                 'pid\[\d+\], build\[.+\], OS\[.+\], JVM\[.+\]',
                                                 line_ctx))
                    elif lineIdx == 4:
                        self.assertTrue(re.match(r'JVM arguments \[.+\]', line_ctx))
                    elif lineIdx == 5:
                        self.assertTrue('initialized', line_ctx)
                    elif lineIdx == 6:
                        self.assertTrue('starting ...', line_ctx)
                    elif lineIdx == 7:
                        self.assertTrue('started', line_ctx)

    @staticmethod
    def create_log_from_template(log_file_path, tmp_home):
        with open(Path(Path(__file__).parent, "log4j2_file.properties"), "r") as fin, \
             open(Path(tmp_home, "config", "log4j2.properties"), "w") as fout:
            for line in fin:
                fout.write(line.replace("<log_file_path>", str(log_file_path)))
