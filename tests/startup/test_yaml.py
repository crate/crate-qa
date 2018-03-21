import os
import shutil
import unittest
from pathlib import Path
from crate.client import connect
from crate.qa.tests import NodeProvider
from faker import Faker
from faker.config import AVAILABLE_LOCALES
from faker.generator import random


class StartupTest(NodeProvider, unittest.TestCase):

    fake = Faker(random.choice(list(AVAILABLE_LOCALES)))

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
        with connect(node.http_url, error_trace=True) as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT name FROM sys.nodes
            ''')
            res = cur.fetchone()
            self.assertTrue(res[0], crate_yml['node.name'])
