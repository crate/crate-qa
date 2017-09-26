#!/usr/bin/env python3

import os
import unittest
from crate.client import connect
from crate.qa.tests import NodeProvider
from faker import Faker
from faker.config import AVAILABLE_LOCALES
from faker.generator import random


class StartupTest(NodeProvider, unittest.TestCase):

    CRATE_VERSION = os.environ.get('CRATE_VERSION', 'latest-nightly')
    fake = Faker(random.choice(list(AVAILABLE_LOCALES)))

    def test_name_settings(self):
        settings = {
            'node.name': self.fake.name(),
            'cluster.name': self.fake.password(),
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
