from tests.integration.library.loader.test_repository_loader_relation_pass_one import (
    TestRepositoryLoaderRelationPassOne,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderRelationPassOne(
    SqliteRepositoryTestCase, TestRepositoryLoaderRelationPassOne
):
    # Run all tests in TestRepositoryLoaderRelationPassOne
    pass
