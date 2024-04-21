from tests.integration.library.loader.test_repository_loader_relation import (
    TestRepositoryLoaderRelation,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderRelation(
    SqliteRepositoryTestCase, TestRepositoryLoaderRelation
):
    # Run all tests in TestRepositoryLoaderRelation
    pass
