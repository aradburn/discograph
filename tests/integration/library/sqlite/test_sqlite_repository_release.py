from tests.integration.library.database.test_repository_release import (
    TestRepositoryRelease,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryRelease(SqliteRepositoryTestCase, TestRepositoryRelease):
    # Run all tests in TestRepositoryRelease
    pass
