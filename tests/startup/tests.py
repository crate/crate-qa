import os
import socket
import shutil
import unittest
from pathlib import Path
from crate.client import connect
from crate.client.exceptions import ProgrammingError
from crate.qa.tests import NodeProvider
from faker import Faker
from faker.config import AVAILABLE_LOCALES
from faker.generator import random


def _bool(b):
    return 'true' if b else 'false'


class StartupTest(NodeProvider, unittest.TestCase):

    CRATE_VERSION = os.environ.get('CRATE_VERSION', 'latest-nightly')
    fake = Faker(random.choice(list(AVAILABLE_LOCALES)))

    def test_name_settings(self):
        settings = {
            'node.name': self.fake.name(),
            'cluster.name': self.fake.bothify(text='????.##'),
        }
        node = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()
        with connect(node.http_url) as conn:
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
        with connect(node.http_url) as conn:
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

    def test_module_settings(self):
        is_enterprise = random.getrandbits(1)
        settings = {
            'license.enterprise': _bool(is_enterprise),
        }
        if is_enterprise:
            settings.update({
                'lang.js.enabled': _bool(random.getrandbits(1)),
                'ingestion.mqtt.enabled': _bool(random.getrandbits(1)),
                'auth.host_based.enabled': _bool(random.getrandbits(1)),
                'auth.host_based.config.0.user': 'crate',
                'auth.host_based.config.0.host': '127.0.0.1',
                'auth.host_based.config.0.protocol': 'http',
            })

        node = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()

        with connect(node.http_url) as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT settings['license']['enterprise'] AS enabled,
                       settings['license']['ident'] AS ident
                FROM sys.cluster
            ''')
            res = cur.fetchone()
            self.assertEqual(res[0], is_enterprise)
            self.assertEqual(res[1], '')

            if is_enterprise:
                self._test_enterprise_enabled(settings, conn)
            else:
                self._test_enterprise_disabled(settings, conn)

    def _test_enterprise_enabled(self, settings, conn):
        cur = conn.cursor()
        # User Management
        cur.execute('''
            SELECT name, superuser FROM sys.users
        ''')
        res = cur.fetchone()
        self.assertEqual(res[0], 'crate')
        self.assertEqual(res[1], True)

        # UDF Javascript
        if settings['lang.js.enabled'] == 'true':
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
        if settings['ingestion.mqtt.enabled'] == 'true':
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                result = s.connect_ex(('127.0.0.1', 1883))
                self.assertEqual(result, 0)

    def _test_enterprise_disabled(self, settings, conn):
        cur = conn.cursor()
        # User Management
        with self.assertRaisesRegex(ProgrammingError,
                                    'Table \'sys.users\' unknown'):
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            result = s.connect_ex(('127.0.0.1', 1883))
            self.assertTrue(result > 0)

    def test_read_crate_yml(self):
        # Create CRATE_HOME directory
        tmp_home = self.mkdtemp()
        Path(tmp_home, 'config').mkdir()
        # Write crate.yml
        crate_yml = {
            'node.name': self.fake.last_name(),
        }
        with Path(tmp_home, 'config', 'crate.yml').open('w') as fp:
            for k, v in crate_yml.items():
                fp.write(f'{k}: {v}\n')
        # Copy log4j2.properties
        shutil.copyfile(Path(Path(__file__).parent, 'log4j2.properties'),
                        Path(tmp_home, 'config', 'log4j2.properties'))

        settings = {
            'path.home': tmp_home,
        }
        node = self._new_node(self.CRATE_VERSION, settings=settings)
        node.start()
        with connect(node.http_url) as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT name FROM sys.nodes
            ''')
            res = cur.fetchone()
            self.assertTrue(res[0], crate_yml['node.name'])


def test_suite():
    """
    To be executed with `python -m unittest` from same directory as this file.
    """
    return unittest.makeSuite(StartupTest)


if __name__ == '__main__':
    """
    To be executed from anywhere using `python path/to/tests.py`.
    """
    unittest.main(verbosity=2)
