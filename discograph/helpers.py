import atexit
import multiprocessing
import pathlib
import re

from pg_temp import TempDB
from playhouse import pool
from playhouse.postgres_ext import PostgresqlExtDatabase
from playhouse.sqlite_ext import SqliteExtDatabase

from discograph.config import DatabaseType, ThreadingModel
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.database_helper import DatabaseHelper
from discograph.library.discogs_model import database_proxy
from discograph.library.postgres.postgres_bootstrapper import PostgresBootstrapper
from discograph.library.postgres.postgres_helper import PostgresHelper
from discograph.library.sqlite.sqlite_bootstrapper import SqliteBootstrapper
from discograph.library.sqlite.sqlite_helper import SqliteHelper

db_helper: DatabaseHelper

bootstrap_database = None

# noinspection PyTypeChecker
postgres_db: TempDB = None

# noinspection PyTypeChecker
threading_model: ThreadingModel = None

urlify_pattern = re.compile(r"\s+", re.MULTILINE)

args_roles_pattern = re.compile(r'^roles(\[\d*\])?$')


def parse_request_args(args):
    from discograph.library import CreditRole
    year = None
    roles = set()
    for key in args:
        if key == 'year':
            year = args[key]
            try:
                if '-' in year:
                    start, _, stop = year.partition('-')
                    year = tuple(sorted((int(start), int(stop))))
                else:
                    year = int(year)
            finally:
                pass
        elif args_roles_pattern.match(key):
            value = args.getlist(key)
            for role in value:
                if role in CreditRole.all_credit_roles:
                    roles.add(role)
    roles = list(sorted(roles))
    return roles, year


def setup_database(config):
    global db_helper
    global postgres_db
    global bootstrap_database
    global threading_model

    # Based on configuration, use a different database.
    if config['DATABASE'] == DatabaseType.POSTGRES:
        print("Using Postgres Database")
        db_helper = PostgresHelper
        threading_model = config["THREADING_MODEL"]

        options = {
            # "work_mem": "1000MB",
            # "effective_cache_size": "1000MB",
            "max_connections": 20,
            "shared_buffers": "4000MB",
            "log_min_duration_statement": 5000,
            "shared_preload_libraries": 'pg_stat_statements',
            "session_preload_libraries": 'auto_explain',
        }
        postgres_db = TempDB(
            verbosity=0,
            databases=[config['POSTGRES_DATABASE_NAME']],
            initdb=config['POSTGRES_ROOT'] + '/bin/initdb',
            postgres=config['POSTGRES_ROOT'] + '/bin/postgres',
            psql=config['POSTGRES_ROOT'] + '/bin/psql',
            createuser=config['POSTGRES_ROOT'] + '/bin/createuser',
            options=options
        )
        atexit.register(shutdown_database)

        # Create a database instance that will manage the connection and execute queries
        database = pool.PooledPostgresqlExtDatabase(
            config['POSTGRES_DATABASE_NAME'],
            host=postgres_db.pg_socket_dir,
            user=postgres_db.current_user,
            max_connections=20,
        )
        # Configure the proxy database to use the database specified in config.
        database_proxy.initialize(database)

        if config['TESTING']:
            Bootstrapper.is_test = True
        with database.connection_context():
            database.execute_sql("SET auto_explain.log_analyze TO on;")
            database.execute_sql("SET auto_explain.log_min_duration TO 500;")
            database.execute_sql("CREATE EXTENSION pg_stat_statements;")

        bootstrap_database = PostgresqlExtDatabase(
            config['POSTGRES_DATABASE_NAME'],
            host=postgres_db.pg_socket_dir,
            user=postgres_db.current_user,
            autoconnect=False,
        )

        PostgresBootstrapper.bootstrap_models(pessimistic=True)
    elif config['DATABASE'] == DatabaseType.SQLITE:
        print("Using Sqlite Database")
        db_helper = SqliteHelper
        threading_model = config["THREADING_MODEL"]

        database = SqliteExtDatabase(config['SQLITE_DATABASE_NAME'], pragmas={
            'journal_mode': 'wal',
            # 'check_same_thread': False,
            # 'journal_mode': 'off',
            'synchronous': 0,
            'cache_size': 1000000,
            # 'locking_mode': 'exclusive',
            'temp_store': 'memory',
        }, timeout=20)
        # Configure the proxy database to use the database specified in config.
        database_proxy.initialize(database)

        if config['TESTING']:
            Bootstrapper.is_test = True
        if not pathlib.Path(config['SQLITE_DATABASE_NAME']).is_file():
            SqliteBootstrapper.bootstrap_models(pessimistic=True)


def shutdown_database():
    global postgres_db

    print("Shutting down database")
    if postgres_db is not None:
        print("Cleaning up Postgres Database")
        postgres_db.cleanup()
        postgres_db = None


def get_concurrency_count():
    if threading_model == ThreadingModel.PROCESS:
        return multiprocessing.cpu_count()
    elif threading_model == ThreadingModel.THREAD:
        return 1
    else:
        NotImplementedError("THREADING_MODEL not configured")

# def create_worker_class():
#     from discograph.app import app
#     from discograph.config import ThreadingModel
#
#     if app.config["THREADING_MODEL"] == ThreadingModel.THREAD:
#         from multiprocessing.dummy import Process
#     elif app.config["THREADING_MODEL"] == ThreadingModel.PROCESS:
#         from multiprocessing import Process
#     else:
#         raise NotImplementedError(app.config["THREADING_MODEL"])
#     return Process
#
#
# AbstractThread = create_worker_class()
