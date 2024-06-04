import logging
import multiprocessing
from typing import Type

from sqlalchemy.orm import sessionmaker

from discograph.config import DatabaseType, ThreadingModel
from discograph.library.database.database_helper import DatabaseHelper

log = logging.getLogger(__name__)

threading_model: ThreadingModel | None = None


def setup_database(config) -> Type[DatabaseHelper]:
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
    # a sessionmaker(), also in the same scope as the engine
    DatabaseHelper.session_factory = sessionmaker(bind=engine)

    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARN)

    # Check database connection
    db_helper.check_connection(config, engine)

    # Create tables
    db_helper.create_tables()

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

    db_helper.shutdown_database()


def get_concurrency_count():
    if threading_model == ThreadingModel.PROCESS:
        return multiprocessing.cpu_count() * 2
    elif threading_model == ThreadingModel.THREAD:
        return 1
    else:
        NotImplementedError("THREADING_MODEL not configured")
