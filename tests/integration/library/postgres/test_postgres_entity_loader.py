import logging
import unittest

log = logging.getLogger(__name__)


class TestPostgresEntityLoader(unittest.TestCase):
    def setUp(self):
        pass

    # def setUp(self):
    #     setup_logging(is_testing=True)
    #     log.info("setup temp postgres DB")
    #     database.setup_database(vars(PostgresTestConfiguration), bootstrap=False)
    #
    # def tearDown(self):
    #     log.info("cleanup temp postgres DB")
    #     database.shutdown_database()
    #
    # def test_loader_pass_one_01(self):
    #     PostgresEntity.drop_table(True)
    #     PostgresEntity.create_table(True)
    #
    #     log.debug("entity pass 1")
    #     PostgresEntity.database().execute_sql(
    #         "ALTER TABLE postgresentity SET UNLOGGED;"
    #     )
    #     PostgresEntity.loader_pass_one()
    #     PostgresEntity.database().execute_sql("ALTER TABLE postgresentity SET LOGGED;")
    #     PostgresEntity.database().execute_sql("ANALYZE postgresentity;")
    #
    # def test_loader_pass_two_01(self):
    #     PostgresEntity.drop_table(True)
    #     PostgresEntity.create_table(True)
    #
    #     log.debug("entity pass 1")
    #     PostgresEntity.database().execute_sql(
    #         "ALTER TABLE postgresentity SET UNLOGGED;"
    #     )
    #     PostgresEntity.loader_pass_one()
    #     PostgresEntity.database().execute_sql("ALTER TABLE postgresentity SET LOGGED;")
    #     PostgresEntity.database().execute_sql("ANALYZE postgresentity;")
    #
    #     log.debug("entity pass 2")
    #     PostgresEntity.loader_pass_two()
