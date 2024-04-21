from tests.integration.library.loader.test_repository_loader_role import (
    TestRepositoryLoaderRole,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderRole(
    SqliteRepositoryTestCase, TestRepositoryLoaderRole
):
    # Run all tests in TestRepositoryLoaderRole
    pass
