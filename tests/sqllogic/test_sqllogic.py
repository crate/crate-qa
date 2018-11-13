#!/usr/bin/env python3

import os
import re
import faulthandler
import logging
import pathlib
import unittest
from concurrent.futures import ProcessPoolExecutor, as_completed
from os.path import dirname

from crate.qa.tests import NodeProvider, gen_id
from sqllogic.sqllogictest import run_file

here = dirname(__file__)  # tests/sqllogic
project_root = dirname(dirname(here))

tests_path = pathlib.Path(os.path.abspath(os.path.join(
    project_root, 'tests', 'sqllogic', 'testfiles', 'test')))

# Enable to be able to dump threads in case something gets stuck
faulthandler.enable()

# might want to change this to a blacklist at some point
FILE_WHITELIST = [re.compile(o) for o in [
    r'select[1-5].test',
    r'random/select/slt_good_\d+.test',
    r'random/groupby/slt_good_\d+.test',
    r'evidence/slt_lang_createview\.test',
    r'evidence/slt_lang_dropview\.test'
]]


def merge_logfiles(logfiles):
    with open(os.path.join(here, 'sqllogic.log'), 'w') as fw:
        for logfile in logfiles:
            with open(logfile, 'r') as fr:
                content = fr.read()
                if content:
                    fw.write(logfile + '\n')
                    fw.write(content)
            os.remove(logfile)


class SqlLogicTest(NodeProvider, unittest.TestCase):
    CLUSTER_SETTINGS = {
        'cluster.name': gen_id(),
    }

    def test_sqllogic(self):
        """ Runs sqllogictests against latest CrateDB. """
        (node, _) = self._new_node(self.CRATE_VERSION)
        node.start()
        psql_addr = node.addresses.psql
        logfiles = []
        try:
            with ProcessPoolExecutor() as executor:
                futures = []
                for i, filename in enumerate(tests_path.glob('**/*.test')):
                    filepath = tests_path / filename
                    relpath = str(filepath.relative_to(tests_path))
                    if not any(p.match(str(relpath)) for p in FILE_WHITELIST):
                        continue

                    logfile = os.path.join(here, f'sqllogic-{os.path.basename(relpath)}-{i}.log')
                    logfiles.append(logfile)
                    future = executor.submit(
                        run_file,
                        filename=str(filepath),
                        host='localhost',
                        port=str(psql_addr.port),
                        log_level=logging.WARNING,
                        log_file=logfile,
                        failfast=True,
                        schema=f'x{i}'
                    )
                    futures.append(future)
                for future in as_completed(futures):
                    future.result()
        finally:
            # instead of having dozens file merge to one which is in gitignore
            merge_logfiles(logfiles)
