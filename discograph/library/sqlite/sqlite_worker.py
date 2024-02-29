import logging
import pathlib

from peewee import Database
from playhouse.sqlite_ext import SqliteExtDatabase

from discograph.config import Configuration
from discograph.library.database.database_worker import DatabaseWorker


log = logging.getLogger(__name__)


class SqliteWorker(DatabaseWorker):
    @staticmethod
    def setup_worker_database(config: Configuration) -> Database:
        log.info("Using Sqlite Database")

        target_path = pathlib.Path(config["SQLITE_DATABASE_NAME"])
        target_parent = target_path.parent
        target_parent.mkdir(parents=True, exist_ok=True)
        log.info(f"Sqlite Database: {target_path}")

        loader_database = SqliteExtDatabase(
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

        return loader_database

    @staticmethod
    def shutdown_worker_database():
        pass
