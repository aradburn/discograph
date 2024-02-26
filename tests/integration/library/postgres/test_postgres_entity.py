from tests.integration.library.database.test_database_entity import TestDatabaseEntity
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase


class TestPostgresEntity(PostgresTestCase, TestDatabaseEntity):
    # Run all tests in TestDatabaseEntity
    pass
