import unittest

from discograph import database
from discograph.config import PostgresTestConfiguration
from discograph.library.postgres.postgres_entity import PostgresEntity


class TestPostgresEntityBootstrap(unittest.TestCase):

    def setUp(self):
        print("setup temp postgres DB", flush=True)
        database.setup_database(vars(PostgresTestConfiguration), bootstrap=False)

    def tearDown(self):
        print("cleanup temp postgres DB", flush=True)
        database.shutdown_database()

    # noinspection PyPep8
    def test_bootstrap_pass_one_01(self):
        PostgresEntity.drop_table(True)
        PostgresEntity.create_table(True)

        print("entity pass 1")
        PostgresEntity._meta.database.execute_sql("ALTER TABLE postgresentity SET UNLOGGED;")
        PostgresEntity.bootstrap_pass_one()
        PostgresEntity._meta.database.execute_sql("ALTER TABLE postgresentity SET LOGGED;")
        PostgresEntity._meta.database.execute_sql("ANALYZE postgresentity;")

    def test_bootstrap_pass_two_01(self):
        PostgresEntity.drop_table(True)
        PostgresEntity.create_table(True)

        print("entity pass 1")
        PostgresEntity._meta.database.execute_sql("ALTER TABLE postgresentity SET UNLOGGED;")
        PostgresEntity.bootstrap_pass_one()
        PostgresEntity._meta.database.execute_sql("ALTER TABLE postgresentity SET LOGGED;")
        PostgresEntity._meta.database.execute_sql("ANALYZE postgresentity;")

        print("entity pass 2")
        PostgresEntity.bootstrap_pass_two()
