import threading
import queue
import psycopg2.pool


def db_args(parser):
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=str, default='5432')


def db_connection(host=None, port=None, dbname=None, args=None):
    if args:
        host = args.host
        port = int(args.port)
    else:
        host = host or 'localhost'
        port = port and int(port) or 5432
    dbname = dbname or 'doc'
    return psycopg2.connect(host=host, port=port, dbname=dbname)


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

    def __init__(self, num_threads, **kwargs):
        self.num_threads = num_threads
        self.conn_args = kwargs
        self.queue = queue.Queue(num_threads*5)
        self._initThreads()


    def _initThreads(self):
        for i in range(self.num_threads):
            t = ExThread(i, self.queue, db_connection(**self.conn_args))
            t.start()

    def execute(self, stmt, cb):
        self.queue.put((stmt, cb))

    def join(self):
        self.queue.join()
