import logging
import pathlib

from peewee import Database
from playhouse.sqlite_ext import SqliteExtDatabase

from discograph.config import Configuration
from discograph.library.database.database_loader import DatabaseLoader


log = logging.getLogger(__name__)


class SqliteLoader(DatabaseLoader):
    @staticmethod
    def setup_loader_database(config: Configuration) -> Database:
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
    def shutdown_loader_database():
        pass

    @staticmethod
    def load_tables(date: str):
        from discograph.library.sqlite.sqlite_entity import SqliteEntity
        from discograph.library.sqlite.sqlite_relation import SqliteRelation
        from discograph.library.sqlite.sqlite_release import SqliteRelease

        log.info("Load Sqlite tables")

        log.debug("entity pass 1")
        SqliteEntity.loader_pass_one(date)

        log.debug("release pass 1")
        SqliteRelease.loader_pass_one(date)

        log.debug("entity pass 2")
        SqliteEntity.loader_pass_two()

        log.debug("release pass 2")
        SqliteRelease.loader_pass_two()

        log.debug("relation pass 1")
        SqliteRelation.loader_pass_one(date)

        log.debug("entity pass 3")
        SqliteEntity.loader_pass_three()

        log.info("Sqlite loading done.")

    @staticmethod
    def create_tables():
        from discograph.library.sqlite.sqlite_entity import SqliteEntity
        from discograph.library.sqlite.sqlite_relation import SqliteRelation
        from discograph.library.sqlite.sqlite_release import SqliteRelease

        log.info("Load Sqlite tables")

        # Set parameter to True so that the create table query
        # will include an IF NOT EXISTS clause.
        SqliteEntity.create_table(True)
        SqliteRelease.create_table(True)
        SqliteRelation.create_table(True)

    @staticmethod
    def drop_tables():
        from discograph.library.sqlite.sqlite_entity import SqliteEntity
        from discograph.library.sqlite.sqlite_relation import SqliteRelation
        from discograph.library.sqlite.sqlite_release import SqliteRelease

        log.info("Load Sqlite tables")

        SqliteEntity.drop_table(True)
        SqliteRelease.drop_table(True)
        SqliteRelation.drop_table(True)
