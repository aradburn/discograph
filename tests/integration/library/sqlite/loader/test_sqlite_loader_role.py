from tests.integration.library.loader.test_loader_role import (
    TestLoaderRole,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderRole(SqliteRepositoryTestCase, TestLoaderRole):
    # Run all tests in TestLoaderRole
    pass
