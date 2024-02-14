import logging
import multiprocessing
import os
import pathlib
import shutil
import sys

import peewee
import psycopg2
from pg_temp import TempDB
from playhouse import pool
from playhouse.cockroachdb import CockroachDatabase, PooledCockroachDatabase
from playhouse.postgres_ext import PostgresqlExtDatabase
from playhouse.sqlite_ext import SqliteExtDatabase

from discograph.config import DatabaseType, ThreadingModel
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.database_helper import DatabaseHelper
from discograph.library.discogs_model import database_proxy

log = logging.getLogger(__name__)

db_helper: DatabaseHelper

bootstrap_database = None

# noinspection PyTypeChecker
postgres_db: TempDB = None

# noinspection PyTypeChecker
threading_model: ThreadingModel = None


def setup_database(config, bootstrap=True):
    global db_helper
    global postgres_db
    global bootstrap_database
    global threading_model

    # Based on configuration, use a different database.
    if config["DATABASE"] == DatabaseType.POSTGRES:
        from discograph.library.postgres.postgres_helper import PostgresHelper

        log.info("")
        log.info("Using Postgres Database")
        log.info("")
        db_helper = PostgresHelper
        threading_model = config["THREADING_MODEL"]

        if config["PRODUCTION"]:
            log.info("**************************************")
            log.info("* Using Production Postgres Database *")
            log.info("**************************************")
            log.info("")

            bootstrap = False

            # Create a database instance that will manage the connection and execute queries
            database = pool.PooledPostgresqlExtDatabase(
                config["POSTGRES_DATABASE_NAME"],
                host=config["POSTGRES_DATABASE_HOST"],
                port=config["POSTGRES_DATABASE_PORT"],
                user=config["POSTGRES_DATABASE_USERNAME"],
                password=config["POSTGRES_DATABASE_PASSWORD"],
                max_connections=40,
                timeout=300,  # 5 minutes.
                stale_timeout=300,  # 5 minutes.
            )
            # Configure the proxy database to use the database specified in config.
            database_proxy.initialize(database)

            # with database.connection_context():
            # database.execute_sql("SET auto_explain.log_analyze TO on;")
            # database.execute_sql("SET auto_explain.log_min_duration TO 500;")
            # database.execute_sql("CREATE EXTENSION pg_stat_statements;")

            bootstrap_database = PostgresqlExtDatabase(
                config["POSTGRES_DATABASE_NAME"],
                host=config["POSTGRES_DATABASE_HOST"],
                port=config["POSTGRES_DATABASE_PORT"],
                user=config["POSTGRES_DATABASE_USERNAME"],
                password=config["POSTGRES_DATABASE_PASSWORD"],
                autoconnect=False,
            )
        else:
            dirname = config["POSTGRES_DATA"]
            pg_data_dir = os.path.join(dirname, "data")

            data_path = pathlib.Path(pg_data_dir)
            # pg_data_path = pathlib.Path(pg_data_dir)
            # if config['TESTING']:
            #     data_path.rmdir()
            data_path.parent.mkdir(parents=True, exist_ok=True)

            options = {
                "work_mem": "500MB",
                "maintenance_work_mem": "500MB",
                "effective_cache_size": "4GB",
                "max_connections": 34,
                "shared_buffers": "2GB",
                # "log_min_duration_statement": 5000,
                # "shared_preload_libraries": 'pg_stat_statements',
                # "session_preload_libraries": 'auto_explain',
            }
            postgres_db = TempDB(
                verbosity=0,
                databases=[config["POSTGRES_DATABASE_NAME"]],
                initdb=config["POSTGRES_ROOT"] + "/bin/initdb",
                postgres=config["POSTGRES_ROOT"] + "/bin/postgres",
                psql=config["POSTGRES_ROOT"] + "/bin/psql",
                createuser=config["POSTGRES_ROOT"] + "/bin/createuser",
                dirname=dirname,
                options=options,
            )

            # Create a database instance that will manage the connection and execute queries
            database = pool.PooledPostgresqlExtDatabase(
                config["POSTGRES_DATABASE_NAME"],
                host=postgres_db.pg_socket_dir,
                user=postgres_db.current_user,
                max_connections=8,
            )
            # Configure the proxy database to use the database specified in config.
            database_proxy.initialize(database)
            # with database.connection_context():
            # database.execute_sql("SET auto_explain.log_analyze TO on;")
            # database.execute_sql("SET auto_explain.log_min_duration TO 500;")
            # database.execute_sql("CREATE EXTENSION pg_stat_statements;")

            bootstrap_database = PostgresqlExtDatabase(
                config["POSTGRES_DATABASE_NAME"],
                host=postgres_db.pg_socket_dir,
                user=postgres_db.current_user,
                autoconnect=False,
            )

        if config["TESTING"]:
            Bootstrapper.is_test = True

        if bootstrap:
            from discograph.library.postgres.postgres_bootstrapper import PostgresBootstrapper

            PostgresBootstrapper.bootstrap_models()

        db_logger = logging.getLogger("peewee")
        db_logger.addHandler(logging.StreamHandler(stream=sys.stdout))
        db_logger.setLevel(logging.INFO)

        check_postgres_connection(config, database)

    elif config["DATABASE"] == DatabaseType.SQLITE:
        from discograph.library.sqlite.sqlite_helper import SqliteHelper

        log.info("Using Sqlite Database")
        db_helper = SqliteHelper
        threading_model = config["THREADING_MODEL"]

        target_path = pathlib.Path(config["SQLITE_DATABASE_NAME"])
        target_parent = target_path.parent
        target_parent.mkdir(parents=True, exist_ok=True)
        log.info(f"Sqlite Database: {target_path}")

        database = SqliteExtDatabase(
            config["SQLITE_DATABASE_NAME"],
            pragmas={
                "journal_mode": "wal",
                # 'check_same_thread': False,
                # 'journal_mode': 'off',
                "synchronous": 0,
                "cache_size": 1000000,
                # 'locking_mode': 'exclusive',
                "temp_store": "memory",
            },
            timeout=20,
        )
        # Configure the proxy database to use the database specified in config.
        database_proxy.initialize(database)

        if config["TESTING"]:
            Bootstrapper.is_test = True

        if bootstrap:
            from discograph.library.sqlite.sqlite_bootstrapper import SqliteBootstrapper

            SqliteBootstrapper.bootstrap_models()
    elif config["DATABASE"] == DatabaseType.COCKROACH:
        from discograph.library.cockroach.cockroach_helper import CockroachHelper

        log.info("Using Cockroach Database")
        db_helper = CockroachHelper
        threading_model = config["THREADING_MODEL"]

        database = PooledCockroachDatabase(
            config["COCKROACH_DATABASE_NAME"],
            user="root",
            host="localhost",
            # sslmode='verify-full',
            # sslrootcert='/opt/cockroachdb/certs/ca.crt',
            # sslcert='/opt/cockroachdb/certs/client.root.crt',
            # sslkey='/opt/cockroachdb/certs/client.root.key',
            max_connections=8,
        )
        # Configure the proxy database to use the database specified in config.
        database_proxy.initialize(database)

        with database.connection_context():
            try:
                database.execute_sql(
                    f"CREATE DATABASE {config['COCKROACH_DATABASE_NAME']};"
                )
            except peewee.PeeweeException:
                pass

        if config["TESTING"]:
            Bootstrapper.is_test = True

        bootstrap_database = CockroachDatabase(
            config["COCKROACH_DATABASE_NAME"],
            user="root",
            host="localhost",
            # sslmode='verify-full',
            # sslrootcert='/opt/cockroachdb/certs/ca.crt',
            # sslcert='/opt/cockroachdb/certs/client.root.crt',
            # sslkey='/opt/cockroachdb/certs/client.root.key',
        )

        if bootstrap:
            from discograph.library.cockroach.cockroach_bootstrapper import CockroachBootstrapper

            CockroachBootstrapper.bootstrap_models()


def shutdown_database():
    global postgres_db, bootstrap_database

    log.info("Shutting down database")
    if postgres_db is not None:
        log.info("Cleaning up Postgres Database")
        postgres_db.cleanup()
        if Bootstrapper.is_test:
            log.info(f"Delete data dir: {postgres_db.pg_data_dir}")
            shutil.rmtree(postgres_db.pg_data_dir)
            log.info(f"Delete socket dir: {postgres_db.pg_socket_dir}")
            shutil.rmtree(postgres_db.pg_socket_dir)
        postgres_db = None
    if bootstrap_database is not None:
        bootstrap_database = None


def check_postgres_connection(config, database):
    try:
        log.info("Check Postgres database connection...")

        if config["PRODUCTION"]:
            connection = psycopg2.connect(
                user=config["POSTGRES_DATABASE_USERNAME"],
                password=config["POSTGRES_DATABASE_PASSWORD"],
                host=config["POSTGRES_DATABASE_HOST"],
                port=config["POSTGRES_DATABASE_PORT"],
                database=config["POSTGRES_DATABASE_NAME"],
            )
            cursor = connection.cursor()
        else:
            cursor = database.cursor()

        cursor.execute("SELECT version();")
        record = cursor.fetchone()

        log.info(f"Database Version: {record}")
        # database.init(config['POSTGRES_DATABASE_NAME'])
        # database.connect()
        # database.close()
        log.info("Database connected OK.")
    except psycopg2.Error as e:
        log.exception("Error: %s" % e)


def get_concurrency_count():
    if threading_model == ThreadingModel.PROCESS:
        return multiprocessing.cpu_count() * 2
    elif threading_model == ThreadingModel.THREAD:
        return 1
    else:
        NotImplementedError("THREADING_MODEL not configured")
