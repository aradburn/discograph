import logging
import multiprocessing
import os

from sqlalchemy import exc
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker, close_all_sessions

from discograph.config import (
    DatabaseType,
    ThreadingModel,
    ALL_RUNTIME_DATABASE_TABLE_NAMES,
)
from discograph.logging_config import LOGGING_TRACE
from discograph.runtime.runtime_database.runtime_database_helper import (
    RuntimeDatabaseHelper,
)

log = logging.getLogger(__name__)


class RuntimeDatabaseManager:
    runtime_db_helper: RuntimeDatabaseHelper | None = None
    _threading_model: ThreadingModel | None = None

    @staticmethod
    def get_concurrency_count() -> int:
        if RuntimeDatabaseManager._threading_model == ThreadingModel.PROCESS:
            return multiprocessing.cpu_count()
        elif RuntimeDatabaseManager._threading_model == ThreadingModel.THREAD:
            return 1
        else:
            raise NotImplementedError("THREADING_MODEL not configured")

    @classmethod
    def setup_database(cls, config) -> None:
        cls._threading_model = config["THREADING_MODEL"]

        # Based on configuration, use a different database.
        if config["DATABASE"] == DatabaseType.POSTGRES:
            from discograph.runtime.postgres.postgres_helper import (
                RuntimePostgresHelper,
            )

            cls.runtime_db_helper = RuntimePostgresHelper()

        elif config["DATABASE"] == DatabaseType.SQLITE:
            from discograph.runtime.sqlite.sqlite_helper import RuntimeSqliteHelper

            cls.runtime_db_helper = RuntimeSqliteHelper()

        else:
            raise ValueError("Configuration Error: Unknown database type")

        engine = cls.runtime_db_helper.setup_database(config)
        RuntimeDatabaseHelper.engine = engine

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

        if RuntimeDatabaseManager.get_concurrency_count() > 1:
            listen(engine, "connect", engine_on_connect)
            listen(engine, "checkout", engine_on_checkout)

        # a sessionmaker(), also in the same scope as the engine
        RuntimeDatabaseHelper.session_factory = sessionmaker(bind=engine)

        # Set logging level for SqlAlchemy
        # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARN)

        # Check database connection
        cls.runtime_db_helper.check_connection(config, engine)

        # Load data from tables
        cls.runtime_db_helper.load_tables()

    @classmethod
    def shutdown_database(cls):
        log.info("Shutting down database connections")

        close_all_sessions()
        cls.runtime_db_helper.engine.dispose()

        cls.runtime_db_helper.shutdown_database()
