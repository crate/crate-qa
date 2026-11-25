#!/usr/bin/env python3

import logging
import pathlib
import shutil
import subprocess
import unittest
from crate.qa.tests import NodeProvider
from sqllogic.sqllogictest import run_file

INTEG_TESTS_PATH = pathlib.Path(__file__).parent.resolve() / 'integtests'
CRATE_REPO = 'https://github.com/crate/crate.git'
INTEG_TESTS_SRCS = (
    'server/src/test/resources/integtests '
    'extensions/functions/src/test/resources/integtests'
)


def fetch_integ_tests():
    """Sparse-checkout the integtests from the crate/crate repo into
    INTEG_TESTS_PATH."""
    shutil.rmtree(INTEG_TESTS_PATH, ignore_errors=True)
    script = f'''
        set -e
        cd /tmp && rm -rf crate-checkout && mkdir crate-checkout && cd crate-checkout
        git clone --depth 1 --filter=blob:none --no-checkout {CRATE_REPO}
        cd crate
        git sparse-checkout init --cone
        git sparse-checkout set {INTEG_TESTS_SRCS}
        git checkout
        mkdir {INTEG_TESTS_PATH}
        # Keep each source's path, so that test files with the same name in
        # different modules don't overwrite each other.
        for src in {INTEG_TESTS_SRCS}; do
            mkdir -p {INTEG_TESTS_PATH}/$src
            cp -r $src/* {INTEG_TESTS_PATH}/$src/
        done
        cd /tmp && rm -rf crate-checkout
    '''
    subprocess.run(['bash', '-c', script], check=True)


def run_sqllogic_tests(node, schema_prefix='bwc'):
    """Run sqllogic integtests against the given node's PostgreSQL endpoint."""
    psql_port = str(node.addresses.psql.port)
    numShards = 0
    test_files = sorted(INTEG_TESTS_PATH.glob('**/*.test'))
    if not test_files:
        raise FileNotFoundError(
            f'No .test files found in {INTEG_TESTS_PATH}')
    for i, test_file in enumerate(test_files):
        print("")  # force newline for first print
        print(f"Test file {test_file}")
        numShards += run_file(
            filename=str(test_file),
            host='localhost',
            port=psql_port,
            log_level=logging.WARNING,
            log_file=None,
            failfast=True,
            schema=f'{schema_prefix}{i}'
        )
    return numShards


class SqlLogicMixedClusterTest(NodeProvider, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        fetch_integ_tests()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(INTEG_TESTS_PATH, ignore_errors=True)
        super().tearDownClass()

    def test_stable_and_nightly(self):
        settings = {
            'lang.js.enabled': 'true'
        }
        version1 = '6.5.x'
        version2 = 'latest-nightly'
        # Workaround to get mixed cluster
        # Upgrading is not the focus of the test
        cluster = self._new_cluster(version1, num_nodes=3, settings=settings)
        cluster.start()
        old_node = cluster.nodes()[0]
        new_node = self.upgrade_node(old_node, version2)
        old_node = cluster.nodes()[2]
        print(f"    Run sqllogic integ tests on old_node {version1}")
        run_sqllogic_tests(old_node, schema_prefix='mixed_cluster_sqllogic')
        print(f"    Run sqllogic integ tests on new_node {version2}")
        run_sqllogic_tests(new_node, schema_prefix='mixed_cluster_sqllogic')
