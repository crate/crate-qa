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
integtests_path = pathlib.Path(os.path.abspath(os.path.join(
    project_root, 'tests', 'sqllogic', 'integtests')))

# Enable to be able to dump threads in case something gets stuck
faulthandler.enable()

# might want to change this to a blacklist at some point
FILE_WHITELIST = [re.compile(o) for o in [
    r'select[1-5].test',
    r'random/select/slt_good_\d+.test',
    r'random/groupby/slt_good_\d+.test',
    r'random/aggregates/slt_good_\d+.test',
    r'evidence/slt_lang_createview\.test',
    r'evidence/slt_lang_dropview\.test',
    r'custom/tableau.test'
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
                # The upstream sqllogic suite under testfiles/test is filtered
                # by FILE_WHITELIST. tests under integtests/ are always run.
                test_sources = [
                    (tests_path, True),
                    (integtests_path, False),
                ]
                i = 0
                for path, apply_whitelist in test_sources:
                    for filename in path.glob('**/*.test'):
                        filepath = path / filename
                        relpath = str(filepath.relative_to(path))
                        if apply_whitelist and not any(
                                p.match(str(relpath)) for p in FILE_WHITELIST):
                            continue

                        logfile = os.path.join(
                            here, f'sqllogic-{os.path.basename(relpath)}-{i}.log')
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
                        i += 1
                for future in as_completed(futures):
                    future.result()
        finally:
            # instead of having dozens file merge to one which is in gitignore
            merge_logfiles(logfiles)
