import pathlib
import re

from pg_temp import TempDB
from playhouse import pool
from playhouse.sqlite_ext import SqliteExtDatabase

from discograph.config import DatabaseType
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.database_helper import DatabaseHelper
from discograph.library.discogs_model import database_proxy
from discograph.library.postgres.postgres_bootstrapper import PostgresBootstrapper
from discograph.library.postgres.postgres_helper import PostgresHelper
from discograph.library.sqlite.sqlite_bootstrapper import SqliteBootstrapper
from discograph.library.sqlite.sqlite_helper import SqliteHelper

db_helper: DatabaseHelper
postgres_db: TempDB

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

    # Based on configuration, use a different database.
    if config['DATABASE'] == DatabaseType.POSTGRES:
        print("Using Postgres Database")
        db_helper = PostgresHelper
        postgres_db = TempDB(
            verbosity=0,
            databases=[config['POSTGRES_DATABASE_NAME']],
            initdb=config['POSTGRES_ROOT'] + '/bin/initdb',
            postgres=config['POSTGRES_ROOT'] + '/bin/postgres',
            psql=config['POSTGRES_ROOT'] + '/bin/psql',
            createuser=config['POSTGRES_ROOT'] + '/bin/createuser',
        )

        # Create a database instance that will manage the connection and execute queries
        database = pool.PooledPostgresqlExtDatabase(
            config['POSTGRES_DATABASE_NAME'],
            host=postgres_db.pg_socket_dir,
            user=postgres_db.current_user,
        )
        # Configure the proxy database to use the database specified in config.
        database_proxy.initialize(database)
        if config['TESTING']:
            Bootstrapper.is_test = True
        PostgresBootstrapper.bootstrap_models(pessimistic=True)
    elif config['DATABASE'] == DatabaseType.SQLITE:
        print("Using Sqlite Database")
        db_helper = SqliteHelper
        database = SqliteExtDatabase(config['SQLITE_DATABASE_NAME'], pragmas={
            'journal_mode': 'off',
            'synchronous': 0,
            'cache_size': 1000000,
            # 'locking_mode': 'exclusive',
            'temp_store': 'memory',
        })
        # Configure the proxy database to use the database specified in config.
        database_proxy.initialize(database)
        if config['TESTING']:
            Bootstrapper.is_test = True
        if not pathlib.Path(config['SQLITE_DATABASE_NAME']).is_file():
            SqliteBootstrapper.bootstrap_models(pessimistic=True)


def shutdown_database():
    global postgres_db

    print("Shutting down database")
    if postgres_db:
        postgres_db.cleanup()
        postgres_db = None
