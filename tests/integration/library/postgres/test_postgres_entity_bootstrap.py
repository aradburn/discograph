import logging
import unittest

from discograph import database
from discograph.config import PostgresTestConfiguration
from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.logging import setup_logging

log = logging.getLogger(__name__)


class TestPostgresEntityBootstrap(unittest.TestCase):
    def setUp(self):
        setup_logging(is_testing=True)
        log.info("setup temp postgres DB")
        database.setup_database(vars(PostgresTestConfiguration), bootstrap=False)

    def tearDown(self):
        log.info("cleanup temp postgres DB")
        database.shutdown_database()

    # noinspection PyPep8
    def test_bootstrap_pass_one_01(self):
        PostgresEntity.drop_table(True)
        PostgresEntity.create_table(True)

        log.debug("entity pass 1")
        PostgresEntity.database().execute_sql(
            "ALTER TABLE postgresentity SET UNLOGGED;"
        )
        PostgresEntity.bootstrap_pass_one()
        PostgresEntity.database().execute_sql("ALTER TABLE postgresentity SET LOGGED;")
        PostgresEntity.database().execute_sql("ANALYZE postgresentity;")

    def test_bootstrap_pass_two_01(self):
        PostgresEntity.drop_table(True)
        PostgresEntity.create_table(True)

        log.debug("entity pass 1")
        PostgresEntity.database().execute_sql(
            "ALTER TABLE postgresentity SET UNLOGGED;"
        )
        PostgresEntity.bootstrap_pass_one()
        PostgresEntity.database().execute_sql("ALTER TABLE postgresentity SET LOGGED;")
        PostgresEntity.database().execute_sql("ANALYZE postgresentity;")

        log.debug("entity pass 2")
        PostgresEntity.bootstrap_pass_two()
