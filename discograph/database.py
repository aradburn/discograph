import logging
import multiprocessing
import os
from typing import Type

from sqlalchemy import exc
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker, close_all_sessions

from discograph.config import DatabaseType, ThreadingModel, ALL_DATABASE_TABLE_NAMES
from discograph.library.database.database_helper import DatabaseHelper
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)

threading_model: ThreadingModel | None = None


def setup_database(config) -> Type[DatabaseHelper]:
    from discograph.library.loader.loader_role import LoaderRole

    global threading_model

    threading_model = config["THREADING_MODEL"]

    # Based on configuration, use a different database.
    if config["DATABASE"] == DatabaseType.POSTGRES:
        from discograph.library.postgres.postgres_helper import PostgresHelper

        db_helper = PostgresHelper

    elif config["DATABASE"] == DatabaseType.SQLITE:
        from discograph.library.sqlite.sqlite_helper import SqliteHelper

        db_helper = SqliteHelper

    elif config["DATABASE"] == DatabaseType.COCKROACH:
        from discograph.library.cockroach.cockroach_helper import CockroachHelper

        db_helper = CockroachHelper
    else:
        raise ValueError("Configuration Error: Unknown database type")

    DatabaseHelper.db_helper = db_helper

    engine = db_helper.setup_database(config)
    DatabaseHelper.engine = engine

    def engine_on_connect(dbapi_con, connection_record):
        if LOGGING_TRACE:
            log.debug(f"New engine connection: {dbapi_con}")
        connection_record.info["pid"] = os.getpid()

    def engine_on_checkout(dbapi_con, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info["pid"] != pid:
            log.error(f"New engine checkout using wrong pid: {dbapi_con}")

            connection_record.dbapi_connection = connection_proxy.dbapi_connection = (
                None
            )
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s"
                % (connection_record.info["pid"], pid)
            )

    if get_concurrency_count() > 1:
        listen(engine, "connect", engine_on_connect)
        listen(engine, "checkout", engine_on_checkout)

    # a sessionmaker(), also in the same scope as the engine
    DatabaseHelper.session_factory = sessionmaker(bind=engine)

    # Set logging level for SqlAlchemy
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARN)

    # Check database connection
    db_helper.check_connection(config, engine)

    # Create tables
    db_helper.create_tables(ALL_DATABASE_TABLE_NAMES)

    LoaderRole.load_roles_into_database()

    return db_helper


def shutdown_database(config):
    # Based on configuration, use a different database.
    if config["DATABASE"] == DatabaseType.POSTGRES:
        from discograph.library.postgres.postgres_helper import PostgresHelper

        db_helper = PostgresHelper

    elif config["DATABASE"] == DatabaseType.SQLITE:
        from discograph.library.sqlite.sqlite_helper import SqliteHelper

        db_helper = SqliteHelper

    elif config["DATABASE"] == DatabaseType.COCKROACH:
        from discograph.library.cockroach.cockroach_helper import CockroachHelper

        db_helper = CockroachHelper
    else:
        raise ValueError("Configuration Error: Unknown database type")

    log.info("Shutting down database connections")

    close_all_sessions()
    db_helper.engine.dispose()

    db_helper.shutdown_database()


def get_concurrency_count() -> int:
    if threading_model == ThreadingModel.PROCESS:
        return multiprocessing.cpu_count()
    elif threading_model == ThreadingModel.THREAD:
        return 1
    else:
        NotImplementedError("THREADING_MODEL not configured")
