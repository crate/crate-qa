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
    r'random/aggregates/slt_good_\d+.test',
    r'evidence/slt_lang_createview\.test',
    r'evidence/slt_lang_dropview\.test',
    r'custom/tableau.test'
]]


def extract_breaker_errors(logfile, limit=30):
    """Pull the queries that tripped the circuit breaker out of the merged log.

    A breaker error raised by a query from a .test file is logged (with its
    query and testfile) and the run continues - only the harness' own
    bookkeeping queries actually fail the test. So the queries that consumed
    the memory are in sqllogic.log, never in the traceback. Jenkins deletes the
    workspace, so surface them in the failure message instead.
    """
    try:
        with open(logfile, 'r', encoding='utf-8') as f:
            hits = [line.rstrip('\n') for line in f if 'breaker would use' in line]
    except OSError as e:
        return f'\n\n<could not read {logfile}: {e}>'
    if not hits:
        return ('\n\nno breaker errors were logged for individual queries '
                '(the trip came from elsewhere)')
    body = '\n'.join('  ' + line for line in hits[:limit])
    more = '' if len(hits) <= limit else f'\n  ... and {len(hits) - limit} more'
    return (f'\n\nqueries that tripped the breaker during this run '
            f'({len(hits)} total, format "ERROR; <testfile>; <query>; <error>"):'
            f'\n{body}{more}')


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

    def test_sqllogic(self):
        """ Runs sqllogictests against latest CrateDB. """
        CLUSTER_SETTINGS = {
            'cluster.name': gen_id(),
            'indices.breaker.policy': 'top_consumer'
        }
        (node, _) = self._new_node(self.CRATE_VERSION, settings=CLUSTER_SETTINGS)
        node.start()
        psql_addr = node.addresses.psql
        logfiles = []
        failure = None
        try:
            with ProcessPoolExecutor() as executor:
                futures = {}
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
                    futures[future] = relpath
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        failure = (futures[future], e)
                        break
        finally:
            # instead of having dozens file merge to one which is in gitignore
            merge_logfiles(logfiles)
        if failure:
            relpath, error = failure
            details = extract_breaker_errors(os.path.join(here, 'sqllogic.log'))
            raise AssertionError(f'{relpath} failed: {error}{details}') from error
