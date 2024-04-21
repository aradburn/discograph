from tests.integration.library.database.test_repository_role import TestRepositoryRole
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryRole(SqliteRepositoryTestCase, TestRepositoryRole):
    # Run all tests in TestRepositoryRole
    pass
