from tests.integration.library.loader.test_loader_relation_pass_two import (
    TestLoaderRelationPassTwo,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderRelationPassTwo(
    SqliteRepositoryTestCase, TestLoaderRelationPassTwo
):
    # Run all tests in TestLoaderRelationPassTwo
    pass
