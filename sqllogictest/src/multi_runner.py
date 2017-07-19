import argparse
import logging
import sys

from cr8 import run_crate

import sqllogictest

def main():
    parser = argparse.ArgumentParser(prog='multi_runner')
    parser.add_argument('testfiles', nargs='*')
    parser.add_argument('--out', help="output file", type=argparse.FileType('a', encoding='utf-8'), default=sys.stdout)
    args = parser.parse_args()
    versions = ['1.0.x', '1.1.x', '2.0.x', '2.1.x']
    with args.out as outfile:
        for version in versions:
            r = sqllogictest.Runner('localhost', '5432', logging.WARNING, None, False)
            with run_crate.create_node(version, None, None, None, False) as n:
                n.start()
                for testfile in args.testfiles:
                    with open(testfile, 'r', encoding='utf8') as f:
                        stats = r.run_file(f)
                    print(f"{r.version}\t{stats}", file=outfile)
                    outfile.flush()
