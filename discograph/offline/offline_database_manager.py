import logging
import multiprocessing
import os

from sqlalchemy import exc
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker, close_all_sessions

from discograph.config import (
    DatabaseType,
    ThreadingModel,
    ALL_OFFLINE_DATABASE_TABLE_NAMES,
)
from discograph.logging_config import LOGGING_TRACE
from discograph.offline.database.database_helper import DatabaseHelper

log = logging.getLogger(__name__)


class OfflineDatabaseManager:
    db_helper: DatabaseHelper | None = None
    _threading_model: ThreadingModel | None = None

    @staticmethod
    def get_concurrency_count() -> int:
        if OfflineDatabaseManager._threading_model == ThreadingModel.PROCESS:
            return multiprocessing.cpu_count()
        elif OfflineDatabaseManager._threading_model == ThreadingModel.THREAD:
            return 1
        else:
            raise NotImplementedError("THREADING_MODEL not configured")

    @classmethod
    def setup_database(cls, config) -> None:
        from discograph.offline.loader.loader_role import LoaderRole

        cls._threading_model = config["THREADING_MODEL"]

        # Based on configuration, use a different database.
        if config["DATABASE"] == DatabaseType.POSTGRES:
            from discograph.offline.postgres.postgres_helper import PostgresHelper

            cls.db_helper = PostgresHelper()

        elif config["DATABASE"] == DatabaseType.SQLITE:
            from discograph.offline.sqlite.sqlite_helper import SqliteHelper

            cls.db_helper = SqliteHelper()

        else:
            raise ValueError("Configuration Error: Unknown database type")

        engine = cls.db_helper.setup_database(config)
        DatabaseHelper.engine = engine

        def engine_on_connect(dbapi_con, connection_record):
            if LOGGING_TRACE:
                log.debug(f"New engine connection: {dbapi_con}")
            connection_record.info["pid"] = os.getpid()

        def engine_on_checkout(dbapi_con, connection_record, connection_proxy):
            pid = os.getpid()
            if connection_record.info["pid"] != pid:
                log.error(f"New engine checkout using wrong pid: {dbapi_con}")

                connection_record.dbapi_connection = (
                    connection_proxy.dbapi_connection
                ) = None
                raise exc.DisconnectionError(
                    "Connection record belongs to pid %s, "
                    "attempting to check out in pid %s"
                    % (connection_record.info["pid"], pid)
                )

        if OfflineDatabaseManager.get_concurrency_count() > 1:
            listen(engine, "connect", engine_on_connect)
            listen(engine, "checkout", engine_on_checkout)

        # a sessionmaker(), also in the same scope as the engine
        DatabaseHelper.session_factory = sessionmaker(bind=engine)

        # Set logging level for SqlAlchemy
        # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARN)

        # Check database connection
        cls.db_helper.check_connection(config, engine)

        # Create tables
        cls.db_helper.create_tables(ALL_OFFLINE_DATABASE_TABLE_NAMES)

        LoaderRole.load_roles_into_database()

    @classmethod
    def shutdown_database(cls) -> None:
        log.info("Shutting down database connections")

        close_all_sessions()
        cls.db_helper.engine.dispose()

        cls.db_helper.shutdown_database()
