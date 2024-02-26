import logging
import multiprocessing
import sys

from discograph.config import DatabaseType, ThreadingModel
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.database_loader import DatabaseLoader

log = logging.getLogger(__name__)

db_helper: DatabaseHelper
db_loader: DatabaseLoader

# noinspection PyTypeChecker
threading_model: ThreadingModel = None


def setup_database(config):
    global db_helper
    global db_loader
    global threading_model

    threading_model = config["THREADING_MODEL"]

    # Based on configuration, use a different database.
    if config["DATABASE"] == DatabaseType.POSTGRES:
        from discograph.library.postgres.postgres_helper import PostgresHelper
        from discograph.library.postgres.postgres_loader import PostgresLoader

        # from discograph.library.discogs_model import database_proxy

        db_helper = PostgresHelper
        db_loader = PostgresLoader
        # threading_model = config["THREADING_MODEL"]

        # if config["PRODUCTION"]:
        #     log.info("**************************************")
        #     log.info("* Using Production Postgres Database *")
        #     log.info("**************************************")
        #     log.info("")
        #
        #     bootstrap = False
        #
        #     # Create a database instance that will manage the connection and execute queries
        #     database = pool.PooledPostgresqlExtDatabase(
        #         config["POSTGRES_DATABASE_NAME"],
        #         host=config["POSTGRES_DATABASE_HOST"],
        #         port=config["POSTGRES_DATABASE_PORT"],
        #         user=config["POSTGRES_DATABASE_USERNAME"],
        #         password=config["POSTGRES_DATABASE_PASSWORD"],
        #         max_connections=40,
        #         timeout=300,  # 5 minutes.
        #         stale_timeout=300,  # 5 minutes.
        #     )
        #     # Configure the proxy database to use the database specified in config.
        #     database_proxy.initialize(database)
        #
        #     # with database.connection_context():
        #     # database.execute_sql("SET auto_explain.log_analyze TO on;")
        #     # database.execute_sql("SET auto_explain.log_min_duration TO 500;")
        #     # database.execute_sql("CREATE EXTENSION pg_stat_statements;")
        #
        #     bootstrap_database = PostgresqlExtDatabase(
        #         config["POSTGRES_DATABASE_NAME"],
        #         host=config["POSTGRES_DATABASE_HOST"],
        #         port=config["POSTGRES_DATABASE_PORT"],
        #         user=config["POSTGRES_DATABASE_USERNAME"],
        #         password=config["POSTGRES_DATABASE_PASSWORD"],
        #         autoconnect=False,
        #     )
        # else:
        #     dirname = config["POSTGRES_DATA"]
        #     pg_data_dir = os.path.join(dirname, "data")
        #
        #     data_path = pathlib.Path(pg_data_dir)
        #     # pg_data_path = pathlib.Path(pg_data_dir)
        #     # if config['TESTING']:
        #     #     data_path.rmdir()
        #     data_path.parent.mkdir(parents=True, exist_ok=True)
        #
        #     options = {
        #         "work_mem": "500MB",
        #         "maintenance_work_mem": "500MB",
        #         "effective_cache_size": "4GB",
        #         "max_connections": 34,
        #         "shared_buffers": "2GB",
        #         # "log_min_duration_statement": 5000,
        #         # "shared_preload_libraries": 'pg_stat_statements',
        #         # "session_preload_libraries": 'auto_explain',
        #     }
        #     postgres_db = TempDB(
        #         verbosity=0,
        #         databases=[config["POSTGRES_DATABASE_NAME"]],
        #         initdb=config["POSTGRES_ROOT"] + "/bin/initdb",
        #         postgres=config["POSTGRES_ROOT"] + "/bin/postgres",
        #         psql=config["POSTGRES_ROOT"] + "/bin/psql",
        #         createuser=config["POSTGRES_ROOT"] + "/bin/createuser",
        #         dirname=dirname,
        #         options=options,
        #     )
        #
        #     # Create a database instance that will manage the connection and execute queries
        #     database = pool.PooledPostgresqlExtDatabase(
        #         config["POSTGRES_DATABASE_NAME"],
        #         host=postgres_db.pg_socket_dir,
        #         user=postgres_db.current_user,
        #         max_connections=8,
        #     )
        #     # Configure the proxy database to use the database specified in config.
        #     database_proxy.initialize(database)
        #     # with database.connection_context():
        #     # database.execute_sql("SET auto_explain.log_analyze TO on;")
        #     # database.execute_sql("SET auto_explain.log_min_duration TO 500;")
        #     # database.execute_sql("CREATE EXTENSION pg_stat_statements;")
        #
        #     bootstrap_database = PostgresqlExtDatabase(
        #         config["POSTGRES_DATABASE_NAME"],
        #         host=postgres_db.pg_socket_dir,
        #         user=postgres_db.current_user,
        #         autoconnect=False,
        #     )

        # if config["TESTING"]:
        #     Bootstrapper.is_test = True

        # if bootstrap:
        #     from discograph.library.postgres.postgres_bootstrapper import (
        #         PostgresBootstrapper,
        #     )
        #
        #     PostgresBootstrapper.load_models()

    elif config["DATABASE"] == DatabaseType.SQLITE:
        from discograph.library.sqlite.sqlite_helper import SqliteHelper
        from discograph.library.sqlite.sqlite_loader import SqliteLoader

        # from discograph.library.discogs_model import database_proxy

        # log.info("Using Sqlite Database")
        db_helper = SqliteHelper
        db_loader = SqliteLoader

        # threading_model = config["THREADING_MODEL"]

        # db_helper.setup_database(config, database_proxy)

        # target_path = pathlib.Path(config["SQLITE_DATABASE_NAME"])
        # target_parent = target_path.parent
        # target_parent.mkdir(parents=True, exist_ok=True)
        # log.info(f"Sqlite Database: {target_path}")
        #
        # database = SqliteExtDatabase(
        #     config["SQLITE_DATABASE_NAME"],
        #     pragmas={
        #         "journal_mode": "wal",
        #         # 'check_same_thread': False,
        #         # 'journal_mode': 'off',
        #         "synchronous": 0,
        #         "cache_size": 1000000,
        #         # 'locking_mode': 'exclusive',
        #         "temp_store": "memory",
        #     },
        #     timeout=20,
        # )
        # # Configure the proxy database to use the database specified in config.
        # database_proxy.initialize(database)

        # if config["TESTING"]:
        #     Bootstrapper.is_test = True

        # if bootstrap:
        #     from discograph.library.sqlite.sqlite_bootstrapper import SqliteBootstrapper
        #
        #     SqliteBootstrapper.load_models()
    elif config["DATABASE"] == DatabaseType.COCKROACH:
        from discograph.library.cockroach.cockroach_helper import CockroachHelper
        from discograph.library.cockroach.cockroach_loader import CockroachLoader

        # from discograph.library.discogs_model import database_proxy

        db_helper = CockroachHelper
        db_loader = CockroachLoader

        # threading_model = config["THREADING_MODEL"]

        # db_helper.setup_database(config, database_proxy)

        # database = PooledCockroachDatabase(
        #     config["COCKROACH_DATABASE_NAME"],
        #     user="root",
        #     host="localhost",
        #     # sslmode='verify-full',
        #     # sslrootcert='/opt/cockroachdb/certs/ca.crt',
        #     # sslcert='/opt/cockroachdb/certs/client.root.crt',
        #     # sslkey='/opt/cockroachdb/certs/client.root.key',
        #     max_connections=8,
        # )
        # # Configure the proxy database to use the database specified in config.
        # database_proxy.initialize(database)
        #
        # with database.connection_context():
        #     try:
        #         database.execute_sql(
        #             f"CREATE DATABASE {config['COCKROACH_DATABASE_NAME']};"
        #         )
        #     except peewee.PeeweeException:
        #         pass

        # if config["TESTING"]:
        #     Bootstrapper.is_test = True

        # bootstrap_database = CockroachDatabase(
        #     config["COCKROACH_DATABASE_NAME"],
        #     user="root",
        #     host="localhost",
        #     # sslmode='verify-full',
        #     # sslrootcert='/opt/cockroachdb/certs/ca.crt',
        #     # sslcert='/opt/cockroachdb/certs/client.root.crt',
        #     # sslkey='/opt/cockroachdb/certs/client.root.key',
        # )
        #
        # if bootstrap:
        #     from discograph.library.cockroach.cockroach_bootstrapper import (
        #         CockroachBootstrapper,
        #     )
        #
        #     CockroachBootstrapper.load_models()

    # from discograph.library.discogs_model import database_proxy
    from discograph.library.database.database_proxy import database_proxy

    database = db_helper.setup_database(config)
    DatabaseHelper.database = database
    # db_helper.bind_models(database)

    loader_database = db_loader.setup_loader_database(config)
    DatabaseLoader.loader_database = loader_database

    # Configure the proxy database to use the database specified in config.
    database_proxy.initialize(database)
    # db_helper.bind_models(database)

    db_logger = logging.getLogger("peewee")
    db_logger.addHandler(logging.StreamHandler(stream=sys.stdout))
    db_logger.setLevel(logging.INFO)

    db_helper.check_connection(config, database)


def shutdown_database():
    # global postgres_db, bootstrap_database

    db_loader.shutdown_loader_database()
    db_helper.shutdown_database()
    # log.info("Shutting down database")
    # if postgres_db is not None:
    #     log.info("Cleaning up Postgres Database")
    #     postgres_db.cleanup()
    #     if Bootstrapper.is_test:
    #         log.info(f"Delete data dir: {postgres_db.pg_data_dir}")
    #         shutil.rmtree(postgres_db.pg_data_dir)
    #         log.info(f"Delete socket dir: {postgres_db.pg_socket_dir}")
    #         shutil.rmtree(postgres_db.pg_socket_dir)
    #     postgres_db = None
    # if bootstrap_database is not None:
    #     bootstrap_database = None


def get_concurrency_count():
    if threading_model == ThreadingModel.PROCESS:
        return multiprocessing.cpu_count() * 2
    elif threading_model == ThreadingModel.THREAD:
        return 1
    else:
        NotImplementedError("THREADING_MODEL not configured")
