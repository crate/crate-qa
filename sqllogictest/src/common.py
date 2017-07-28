import threading
import queue
import psycopg2.pool


def db_args(parser):
    parser.add_argument('--dsn', default='crate://localhost:5432')


def split_dsn(dsn):
    parts = dsn.split('://')
    if len(parts) == 2:
        return parts
    return 'crate', dsn


def db_connection(dsn):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


class ExThread(threading.Thread):
    def __init__(self, n, queue, conn):
        super().__init__(name=str(n), daemon=True)
        self.conn = conn
        self.queue = queue

    def run(self):
        while True:
            stmt, cb = self.queue.get()
            try:
                with self.conn.cursor() as c:
                    stmt.execute(c)
            except Exception as e:
                cb(stmt, e)
            cb(stmt, None)
            self.queue.task_done()


class PoolExectuor:
    def __init__(self, num_threads, dsn):
        self.num_threads = num_threads
        self.dsn = dsn
        self.queue = queue.Queue(num_threads * 5)
        self._initThreads()

    def _initThreads(self):
        for i in range(self.num_threads):
            t = ExThread(i, self.queue, db_connection(self.dsn))
            t.start()

    def execute(self, stmt, cb):
        self.queue.put((stmt, cb))

    def join(self):
        self.queue.join()
