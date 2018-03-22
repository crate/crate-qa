import os
import socket
import unittest
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
        node = self._new_node(self.CRATE_VERSION, settings=settings)
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
        node = self._new_node(self.CRATE_VERSION, settings=settings)
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

        node = self._new_node(self.CRATE_VERSION, settings=settings)
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

        node = self._new_node(self.CRATE_VERSION, settings=settings)
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
