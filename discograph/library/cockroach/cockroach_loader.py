import logging

import peewee
from peewee import Database, PeeweeException
from playhouse.cockroachdb import PooledCockroachDatabase

from discograph.config import Configuration

log = logging.getLogger(__name__)


class CockroachLoader:
    @staticmethod
    def setup_loader_database(config: Configuration) -> Database:
        log.info("Using Cockroach loader Database")

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

        try:
            with database.connection_context():
                database.execute_sql(
                    f"CREATE DATABASE {config['COCKROACH_DATABASE_NAME']};"
                )
        except PeeweeException:
            pass

        return database

    @staticmethod
    def shutdown_loader_database():
        pass

    @staticmethod
    def load_tables(date: str):
        from discograph.library.cockroach.cockroach_entity import CockroachEntity
        from discograph.library.cockroach.cockroach_relation import CockroachRelation
        from discograph.library.cockroach.cockroach_release import CockroachRelease

        log.info("Load CockroachDB tables")

        log.debug("entity pass 1")
        CockroachEntity.loader_pass_one(date)

        log.debug("release pass 1")
        CockroachRelease.loader_pass_one(date)

        log.debug("entity pass 2")
        CockroachEntity.loader_pass_two()

        log.debug("release pass 2")
        CockroachRelease.loader_pass_two()

        log.debug("relation pass 1")
        CockroachRelation.loader_pass_one(date)

        log.debug("entity pass 3")
        CockroachEntity.loader_pass_three()

        log.info("CockroachDB loading done.")

    @staticmethod
    def create_tables():
        from discograph.library.cockroach.cockroach_entity import CockroachEntity
        from discograph.library.cockroach.cockroach_relation import CockroachRelation
        from discograph.library.cockroach.cockroach_release import CockroachRelease

        log.info("Create CockroachDB tables")

        # Set parameter to True so that the create table query
        # will include an IF NOT EXISTS clause.
        log.debug("entity add index 1")
        entity_idx1 = CockroachEntity.index(
            CockroachEntity.entity_type, CockroachEntity.name
        )
        CockroachEntity.add_index(entity_idx1)
        log.debug("entity add index 2")
        entity_idx2 = CockroachEntity.index(CockroachEntity.name)
        CockroachEntity.add_index(entity_idx2)
        log.debug("entity add index 3")
        entity_idx3 = CockroachEntity.index(CockroachEntity.search_content)
        CockroachEntity.add_index(entity_idx3)

        CockroachEntity.create_table(True)
        CockroachRelease.create_table(True)
        CockroachRelation.create_table(True)

    @staticmethod
    def drop_tables():
        from discograph.library.cockroach.cockroach_entity import CockroachEntity
        from discograph.library.cockroach.cockroach_relation import CockroachRelation
        from discograph.library.cockroach.cockroach_release import CockroachRelease

        log.info("Drop CockroachDB tables")

        try:
            CockroachEntity.drop_table(True)
            CockroachRelease.drop_table(True)
            CockroachRelation.drop_table(True)
        except peewee.OperationalError:
            log.error("Cannot connect to Cockroach Database")
