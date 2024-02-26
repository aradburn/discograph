from tests.integration.library.database.test_database_release import TestDatabaseRelease
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase


class TestPostgresRelease(PostgresTestCase, TestDatabaseRelease):
    # Run all tests in TestDatabaseRelease
    pass
