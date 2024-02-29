import logging

from peewee import Database, PeeweeException
from playhouse.cockroachdb import PooledCockroachDatabase

from discograph.config import Configuration

log = logging.getLogger(__name__)


class CockroachWorker:
    @staticmethod
    def setup_worker_database(config: Configuration) -> Database:
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
    def shutdown_worker_database():
        pass
