import logging

from peewee import Database
from playhouse.postgres_ext import PostgresqlExtDatabase

from discograph.config import Configuration
from discograph.library.database.database_loader import DatabaseLoader
from discograph.library.discogs_model import DiscogsModel
from discograph.library.postgres.postgres_helper import PostgresHelper

log = logging.getLogger(__name__)


class PostgresLoader(DatabaseLoader):
    @staticmethod
    def setup_loader_database(config: Configuration) -> Database:
        if config["PRODUCTION"]:
            log.info("**************************************")
            log.info("* Using Production Postgres Database *")
            log.info("**************************************")
            log.info("")
            raise Exception("DO NOT USE PRODUCTION DATABASE")

            # _postgres_loader_db = PostgresqlExtDatabase(
            #     config["POSTGRES_DATABASE_NAME"],
            #     host=config["POSTGRES_DATABASE_HOST"],
            #     port=config["POSTGRES_DATABASE_PORT"],
            #     user=config["POSTGRES_DATABASE_USERNAME"],
            #     password=config["POSTGRES_DATABASE_PASSWORD"],
            #     autoconnect=False,
            # )
        else:
            log.info("Using Postgres Database")

            loader_database = PostgresqlExtDatabase(
                config["POSTGRES_DATABASE_NAME"],
                host=PostgresHelper.postgres_db.pg_socket_dir,
                user=PostgresHelper.postgres_db.current_user,
                autoconnect=False,
            )

        # with loader_database.connection_context():
        #     loader_database.execute_sql("SET auto_explain.log_analyze TO on;")
        #     loader_database.execute_sql("SET auto_explain.log_min_duration TO 500;")
        #     loader_database.execute_sql("CREATE EXTENSION pg_stat_statements;")

        return loader_database

    @staticmethod
    def shutdown_loader_database():
        pass

    @staticmethod
    def load_tables(date: str):
        from discograph.library.postgres.postgres_entity import PostgresEntity
        from discograph.library.postgres.postgres_relation import PostgresRelation
        from discograph.library.postgres.postgres_release import PostgresRelease

        with DiscogsModel.connection_context():
            log.info("Load Postgres tables")

            log.debug("entity pass 1")
            PostgresEntity.loader_pass_one(date)

            log.debug("entity analyze")
            PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")

            log.debug("release pass 1")
            PostgresRelease.loader_pass_one(date)

            log.debug("release analyze")
            PostgresRelease.database().execute_sql(
                "VACUUM FULL ANALYZE postgresrelease;"
            )

            log.debug("entity pass 2")
            PostgresEntity.loader_pass_two()

            log.debug("release pass 2")
            PostgresRelease.loader_pass_two()

            log.debug("relation pass 1")
            PostgresRelation.loader_pass_one(date)

            log.debug("relation analyze")
            PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")
            PostgresRelease.database().execute_sql(
                "VACUUM FULL ANALYZE postgresrelease;"
            )
            PostgresRelation.database().execute_sql(
                "VACUUM FULL ANALYZE postgresrelation;"
            )

            log.debug("entity pass 3")
            PostgresEntity.loader_pass_three()

            log.debug("final vacuum analyze")
            PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")
            PostgresRelease.database().execute_sql(
                "VACUUM FULL ANALYZE postgresrelease;"
            )
            PostgresRelation.database().execute_sql(
                "VACUUM FULL ANALYZE postgresrelation;"
            )

            log.info("Postgres loading done.")

    @staticmethod
    def create_tables():
        from discograph.library.postgres.postgres_entity import PostgresEntity
        from discograph.library.postgres.postgres_relation import PostgresRelation
        from discograph.library.postgres.postgres_release import PostgresRelease

        log.info("Create Postgres tables")
        # Set parameter to True so that the create table query
        # will include an IF NOT EXISTS clause.
        PostgresEntity.create_table(True)
        PostgresRelease.create_table(True)
        PostgresRelation.create_table(True)

    @staticmethod
    def drop_tables():
        from discograph.library.postgres.postgres_entity import PostgresEntity
        from discograph.library.postgres.postgres_relation import PostgresRelation
        from discograph.library.postgres.postgres_release import PostgresRelease

        log.info("Drop Postgres tables")
        PostgresEntity.drop_table(True)
        PostgresRelease.drop_table(True)
        PostgresRelation.drop_table(True)
