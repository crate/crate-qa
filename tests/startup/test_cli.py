import os
import unittest
from pathlib import Path

from crate.client import connect
from crate.qa.tests import NodeProvider
from faker import Faker
from faker.config import AVAILABLE_LOCALES
from faker.generator import random


def randbool():
    return True if random.getrandbits(1) else False


class StartupTest(NodeProvider, unittest.TestCase):
    fake = Faker(random.choice(list(AVAILABLE_LOCALES)))

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
                SELECT settings['license']['enterprise'] AS enabled
                FROM sys.cluster
            ''')
            res = cur.fetchone()
            self.assertEqual(res[0], is_enterprise)

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
            self.assertIn(' [' + node_name + '] ', log_line)
            if '[o.e.n.Node               ]' in line:
                return log_line.split(' [' + node_name + '] ')[1]
            return None

        with open(log_file_path, 'r') as f:
            for lineIdx, line in enumerate(f):
                line_ctx = verify_and_extract_content_from_log_line(line)
                if not line_ctx:
                    continue
                if lineIdx == 1:
                    self.assertTrue('initializing', line_ctx)
                elif lineIdx == 2:
                    self.assertRegex(
                        line_ctx,
                        rf'node name \[{node_name}\], node ID \[.+\]\n')
                elif lineIdx == 3:
                    version_str = '.'.join([str(v) for v in version_tuple])
                    self.assertRegex(
                        line_ctx,
                        rf'version\[{version_str}(-SNAPSHOT)?\], pid\[\d+\], build\[.+\], OS\[.+\], JVM\[.+\]')
                elif lineIdx == 5:
                    self.assertRegex(line_ctx, r'JVM arguments \[.+\]')
                elif lineIdx == 6:
                    self.assertTrue('initialized', line_ctx)
                elif lineIdx == 7:
                    self.assertTrue('starting ...', line_ctx)
                elif lineIdx == 8:
                    self.assertTrue('started', line_ctx)

    @staticmethod
    def create_log_from_template(log_file_path, tmp_home):
        with open(Path(Path(__file__).parent, "log4j2_file.properties"), "r") as fin, \
                open(Path(tmp_home, "config", "log4j2.properties"), "w") as fout:
            for line in fin:
                fout.write(line.replace("<log_file_path>", str(log_file_path)))
