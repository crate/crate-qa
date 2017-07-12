import argparse
import sys
import csv
import common
import psycopg2

def insert(fh, stmt, host, port):
    conn = common.db_connection(host, port)
    cursor = conn.cursor()
    with fh, cursor:
        reader = csv.reader(fh, delimiter='\t')
        for row in reader:
            print(row)
            try:
                cursor.execute(stmt, row)
            except psycopg2.InternalError as e:
                if not str(e).startswith('DuplicateKeyException'):
                    raise e

def main():
    parser = argparse.ArgumentParser(prog='inserter')
    common.db_args(parser)
    parser.add_argument('stmt', help="insert statement")
    parser.add_argument('infile', type=argparse.FileType('r', encoding='utf-8'), nargs='?', default=sys.stdin)
    args = parser.parse_args()
    insert(args.infile, args.stmt, args.host, args.port)

