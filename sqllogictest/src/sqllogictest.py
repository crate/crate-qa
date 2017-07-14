"""
Program to execute sqllogictest files against CrateDB.

See https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

This program can only execute "full scripts". "prototype scripts" are not
supported.
"""

import argparse
import logging
import re
import threading
from functools import partial
from hashlib import md5

import common
import psycopg2
import sys
import time

QUERY_WHITELIST = [re.compile(o, re.IGNORECASE) for o in [
    'CREATE INDEX.*',  # CREATE INDEX is not supported, but raises SQLParseException
    '.*BETWEEN.*NULL.*',
    'SELECT - SUM \\( col1 \\) \\* \\+ col1 FROM tab0 cor0 GROUP BY col1, col1',  # Result is not deterministic
]]

KNOWN_BUGS_EXCEPTIONS = [
    'must appear in the GROUP BY clause or be used in an aggregation function',
    'if type of default result argument ',
    "Cannot GROUP BY 'NULL':",
    "java.lang.Long cannot be cast to [Ljava.lang.Object;"
]

varchar_to_string = partial(re.compile('VARCHAR\(\d+\)').sub, 'STRING')
real_to_double = partial(re.compile('REAL').sub, 'DOUBLE')
text_to_string = partial(re.compile('TEXT').sub, 'STRING')


class IncorrectResult(Exception):
    pass


def cratify_stmt(stmt):
    return text_to_string(real_to_double(varchar_to_string(stmt)))


class Command:
    query = None

    def __init__(self, query, line_num, pos):
        self.query = cratify_stmt(query)
        self.line_num = line_num
        self.pos = pos

    def __str__(self):
        return f'{self.pos}:{self.line_num} {self.query}'

class Statement(Command):
    def __init__(self, cmd, line_num, pos):
        """Create a statement

        A statement is usually a DML statement that is expected to either work
        or raise an error

        cmd format is:

            statement [ok | error]
            <statement>
        """
        super().__init__(' '.join(cmd[1:]), line_num, pos)
        self.expect_ok = cmd[0].endswith('ok')

    def execute(self, cursor):
        try:
            cursor.execute(self.query)
        except psycopg2.Error as e:
            if self.expect_ok:
                raise IncorrectResult(e)

    def x__repr__(self):
        return 'Statement<{0:.30}>'.format(self.query)


def validate_hash(rows, formats, expected_values, hash_):
    values = len(rows)
    if values != expected_values:
        raise IncorrectResult(
            'Expected {0} values, got {1}'.format(expected_values, values))
    m = md5()
    for row in rows:
        m.update('{0}'.format(row).encode('ascii'))
        m.update('\n'.encode('ascii'))
    digest = m.hexdigest()
    if digest != hash_:
        raise IncorrectResult('Expected values hashing to {0}. Got {1}\n{2}'.format(
            hash_, digest, rows))


class Query(Command):
    HASHING_RE = re.compile('(\d+) values hashing to ([a-z0-9]+)')
    VALID_RESULT_FORMATS = set('TIR')

    def __init__(self, cmd, line_num, pos):
        """Create a query

        cmd format is:

            query <type-string> <sort-mode> [<label>]
            <the actual query
            can take up multiple lines>
            ----
            <result or num values + hash>

        type-string is one of I, R, T per column where:
            I -> Integer result
            R -> Floating point result
            T -> Text result

        sort-mode is either nosort, rowsort or valuesort.

        label is optional and ignored.

        The result itself is either:

         - The rows transformed to have a single column

            Example:

                2 rows with 2 columns:

                    a| b
                    c| d

                Becomes:

                    a
                    b
                    c
                    d

         - The number of values in the result + a md5 hash of the result
        """
        self.result = None
        for i, line in enumerate(cmd):
            if line.startswith('---'):
                super().__init__(' '.join(cmd[1:i]), line_num, pos)
                self.result = cmd[i + 1:]
                break
        else:
            super().__init__(' '.join(cmd[1:]), line_num, pos)

        __, result_formats, sort, *__ = cmd[0].split()
        if result_formats and not (set(result_formats) & Query.VALID_RESULT_FORMATS):
            raise ValueError(
                'Invalid result format codes: {0}\n{1}'.format(result_formats, cmd))
        self.result_formats = result_formats
        self.sort = sort
        self._init_validation_function()

    def _init_validation_function(self):
        if not self.result:
            return
        if len(self.result) == 1:
            m = Query.HASHING_RE.match(self.result[0])
            if m:
                values, hash_ = m.groups()
                self.validate_result = partial(
                    validate_hash, expected_values=int(values), hash_=hash_)
                return

        self.format_cols(self.result)
        self.validate_result = self.validate_cmp_result

    def validate_cmp_result(self, rows, formats):
        if rows != self.result:
            raise IncorrectResult(
                'Expected rows: {0}. Got {1} query={2}'.format(self.result, rows, self.query))

    def validate_result(self, rows, formats):
        pass

    def format_cell(self, v, fmt):
        if v is None or v == 'NULL':
            return 'NULL'
        if fmt == 'I':
            try:
                return int(v)
            except:
                return v
        if fmt == 'R':
            try:
                return float(v)
            except:
                return v

    def format_cols(self, values):
        for i, v in enumerate(values):
            fmt = self.result_formats[i % len(self.result_formats)]
            values[i] = self.format_cell(v, fmt)

    def format_rows(self, rows):
        rows = [list(row) for row in rows]
        for col, fmt in enumerate(self.result_formats):
            for row in rows:
                row[col] = self.format_cell(row[col], fmt)
        return rows

    def execute(self, cursor):
        cursor.execute(self.query)
        fetched_rows = cursor.fetchall()

        try:
            rows = self.format_rows(fetched_rows)
        except Exception as e:
            import pdb; pdb.set_trace()
            raise e
        if len(rows) > 1 and self.sort == 'rowsort':
            rows = sorted(rows, key=lambda row: [str(c) for c in row])
        # flatten the row values for comparison
        rows = [col for row in rows for col in row]
        if self.sort == 'valuesort':
            rows = sorted(rows, key=lambda v: str(v))
        self.validate_result(rows, self.result_formats)

    def x__repr__(self):
        return 'Query<{0}, {1}, {2:.30}>'.format(
            self.result_formats, self.sort, self.query)


def parse_cmd(cmd, line_num, pos):
    """Parse a command into Statement or Query

    >>> parse_cmd(['statement ok', 'INSERT INTO tab0 VALUES(35,97,1)'])
    Statement

    >>> parse_cmd([
    ...     'query III rowsort',
    ...     'SELECT ALL * FROM tab0 AS cor0',
    ...     '---',
    ...     '9 values hashing to 38a1673e2e09d694c8cec45c797034a7',
    ... ])
    Query

    >>> parse_cmd([
    ...     'skipif mysql # not compatible',
    ...     'query I rowsort label-208',
    ...     'SELECT - col1 / col2 col2 FROM tab1 AS cor0',
    ...     '----',
    ...     '0',
    ...     '0',
    ...     '0'
    ... ])
    Query
    """
    type_ = cmd[0]
    while type_.startswith(('skipif', 'onlyif')):
        cmd.pop(0)
        type_ = cmd[0]
    if type_.startswith('statement'):
        return Statement(cmd, line_num, pos)
    if type_.startswith('query'):
        return Query(cmd, line_num, pos)
    if type_.startswith('halt'):
        return None
    raise ValueError('Could not parse command: {0}'.format(cmd))


def get_commands(lines):
    """Split lines by empty line occurences into lists of lines"""
    command = []
    cmd_line_num = line_num = 0
    for line in lines:
        line_num += 1
        if line.startswith(('#', 'hash-threshold')):
            continue
        line = line.strip()
        if not line or line == '':
            if not command:
                continue
            yield cmd_line_num, command
            command = []
        else:
            if len(command) == 0:
                cmd_line_num = line_num
            command.append(line)
    if command:
        yield cmd_line_num, command


def _exec_on_crate(cmd):
    for line in cmd:
        if line.startswith('skipif crate'):
            return False
        if line.startswith('onlyif') and not line.startswith('onlyif crate'):
            return False
    return True


def _refresh_tables(cursor):
    cursor.execute("select table_name from information_schema.tables where table_schema = 'doc'")
    rows = cursor.fetchall()
    for (table,) in rows:
        cursor.execute('refresh table ' + table)


def _drop_tables(cursor):
    cursor.execute("select table_name from information_schema.tables where table_schema = 'doc'")
    rows = cursor.fetchall()
    for (table,) in rows:
        cursor.execute('drop table ' + table)


class PosFilter(logging.Filter):
    line_num = 0
    pos = -1

    def next_cmd(self, line_num):
        self.pos += 1
        self.line_num = line_num

    def filter(self, record):
        record.pos = self.pos;
        record.testfile = self.current_file
        record.line_num = self.line_num
        return True


class Stats:

    lock = threading.Lock()

    failures = 0
    whitelisted = 0
    lines = 0
    commands = 0
    success = 0
    unsupported = 0

    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return f'{self.filename}\t{self.lines}\t{self.commands}\t{self.success}\t{self.whitelisted}\t{self.unsupported}\t{self.failures}'

    def __setattr__(self, name, value):
        if name in ('failures', 'whitelisted', 'lines', 'commands', 'success', 'unsupported'):
            with self.lock:
                super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)


def version_string(version):
    s = version['number']
    if version['build_snapshot']:
        s += '-' + version['build_hash']
    return s


class Runner:
    conn = None

    def __init__(self, host, port, log_level, log_file, failfast, num_threads):
        self.failfast = failfast
        self.log_file = log_file
        self.log_level = log_level
        self.port = port
        self.host = host
        self.logger = self.get_logger()
        self.posFilter = PosFilter()
        self.logger.addFilter(self.posFilter)
        self.ex = common.PoolExectuor(num_threads, host=self.host, port=self.port)

    def get_logger(self):
        logger = logging.getLogger('sqllogic')
        logger.setLevel(self.log_level)
        handler = logging.FileHandler(self.log_file) if self.log_file else logging.StreamHandler(sys.stderr)
        handler.setLevel(self.log_level)
        handler.setFormatter(logging.Formatter('%(levelname)s %(testfile)s %(message)s'))
        logger.addHandler(handler)
        return logger

    def connect(self):
        if self.conn != None:
            return
        self.conn = common.db_connection(self.host, self.port)
        with self.conn.cursor() as c:
            c.execute('select version from sys.nodes limit 1');
            self.version = version_string(c.fetchone()[0])
        return self.conn

    def run_files(self, paths):
        for p in paths:
            with open(p, 'r', encoding='utf-8') as f:
                stats = self.run_file(f)
                yield stats

    def synchronized_execute(self, cmd, cursor):
        try:
            cmd.execute(cursor)
        except Exception as e:
            self.after_execute(cmd, e)
        self.after_execute(cmd, None)

    def after_execute(self, s_or_q, e):
        msg = None
        try:
            if e is not None:
                msg = str(e).strip()
                raise e
            # self.logger.debug('EX time: {0:.4f}'.format(time.time() - t))
            self.stats.success += 1
        except psycopg2.InternalError as e:
            # msg = str(e).strip()
            if msg.startswith('UnsupportedFeatureException') or msg.endswith(' is not supported'):
                self.logger.info(f'EX UNSUPORTED: {msg} {s_or_q}')
                self.stats.unsupported += 1
            else:
                self.logger.error(f'EX PG ERROR: {msg} {s_or_q}')
                self.stats.failures += 1
                if self.failfast:
                    fail = True
                    for s in KNOWN_BUGS_EXCEPTIONS:
                        if s in msg:
                            fail = False
                            break
                    if fail:
                        import pdb;
                        pdb.set_trace()
                        raise e
        except psycopg2.Error as e:
            self.logger.error(f'EX PG ERROR: {msg} {s_or_q}')
            self.stats.failures += 1
            if self.failfast:
                raise e
        except IncorrectResult as e:
            if not any(p.match(s_or_q.query) for p in QUERY_WHITELIST):
                self.logger.error(f'EX INCORRECT {msg} {s_or_q}')
                self.stats.failures += 1
                if self.failfast:
                    raise e
            else:
                self.stats.whitelisted += 1
                self.logger.info(f'EX WHITELISTED: {msg} {s_or_q}')

    def run_file(self, fh):
        self.connect()
        self.stats = Stats(fh.name)
        self.posFilter.current_file = fh.name
        cursor = self.conn.cursor()
        commands = get_commands(fh)
        commands = (cmd for cmd in commands if _exec_on_crate(cmd[1]))
        dml_done = False
        pos = -1
        try:
            for line_num, cmd in commands:
                pos += 1
                self.stats.lines = line_num
                self.stats.commands += 1
                try:
                    s_or_q = parse_cmd(cmd, line_num, pos)
                except Exception as e:
                    self.logger.error(f'COMMAND PARSE FAILURE: {cmd}')
                    raise e
                if not s_or_q:
                    self.logger.debug('HALT')
                    break
                self.logger.debug(f'EX: {s_or_q}')

                is_dql = isinstance(s_or_q, Query)

                if not dml_done and is_dql:
                    dml_done = True
                    _refresh_tables(cursor)

                t = time.time()
                if is_dql:
                    self.ex.execute(s_or_q, self.after_execute)
                else:
                    self.synchronized_execute(s_or_q, cursor)
        finally:
            self.ex.join()
            _drop_tables(cursor)
            cursor.close()
        return self.stats


def main():
    parser = argparse.ArgumentParser(prog='sqllogictest', description=__doc__)
    parser.add_argument('testfiles', nargs='*')
    common.db_args(parser)
    parser.add_argument('-l', '--log-level',
                        type=int, default=logging.WARNING,
                        help='Python log levels: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50')
    parser.add_argument('--failfast',
                        action='store_true', default=False,
                        help='Fail on first error.')
    parser.add_argument('-n', type=int, default=8, help='Number of parallel queries')
    args = parser.parse_args()

    r = Runner(args.host, args.port, args.log_level, None, args.failfast, args.n)
    for stats in r.run_files(args.testfiles):
        print(f'{r.version}\t{stats}')
