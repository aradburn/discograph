from tests.integration.library.database.test_database_role import TestDatabaseRole
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase


class TestPostgresRole(PostgresTestCase, TestDatabaseRole):
    # Run all tests in TestDatabaseRole
    pass
