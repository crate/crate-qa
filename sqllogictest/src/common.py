import psycopg2


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
