from tests.integration.library.database.test_database_release import (
    TestDatabaseRelease,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresDatabaseRelease(PostgresDatabaseTestCase, TestDatabaseRelease):
    # Run all tests in TestDatabaseRelease
    pass
