import logging

from peewee import Database
from playhouse.postgres_ext import PostgresqlExtDatabase

from discograph.config import Configuration
from discograph.library.database.database_worker import DatabaseWorker
from discograph.library.postgres.postgres_helper import PostgresHelper

log = logging.getLogger(__name__)


class PostgresWorker(DatabaseWorker):
    @staticmethod
    def setup_worker_database(config: Configuration) -> Database:
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
            if config["TESTING"]:
                log.info("Using Postgres Test Database")

                loader_database = PostgresqlExtDatabase(
                    config["POSTGRES_DATABASE_NAME"],
                    host=PostgresHelper.postgres_test_db.pg_socket_dir,
                    user=PostgresHelper.postgres_test_db.current_user,
                    autoconnect=False,
                )
            else:
                log.info("Using Postgres Development Database")

                loader_database = PostgresqlExtDatabase(
                    config["POSTGRES_DATABASE_NAME"],
                    host=config["POSTGRES_DATABASE_HOST"],
                    port=config["POSTGRES_DATABASE_PORT"],
                    user=config["POSTGRES_DATABASE_USERNAME"],
                    password=config["POSTGRES_DATABASE_PASSWORD"],
                    autoconnect=False,
                )

        # with loader_database.connection_context():
        #     loader_database.execute_sql("SET auto_explain.log_analyze TO on;")
        #     loader_database.execute_sql("SET auto_explain.log_min_duration TO 500;")
        #     loader_database.execute_sql("CREATE EXTENSION pg_stat_statements;")

        return loader_database

    @staticmethod
    def shutdown_worker_database():
        pass
