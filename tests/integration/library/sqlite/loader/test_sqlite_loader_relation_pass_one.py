from tests.integration.library.loader.test_loader_relation_pass_one import (
    TestLoaderRelationPassOne,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderRelationPassOne(
    SqliteRepositoryTestCase, TestLoaderRelationPassOne
):
    # Run all tests in TestLoaderRelationPassOne
    pass
