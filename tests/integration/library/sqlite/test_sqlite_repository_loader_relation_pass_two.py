from tests.integration.library.loader.test_repository_loader_relation_pass_two import (
    TestRepositoryLoaderRelationPassTwo,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderRelationPassTwo(
    SqliteRepositoryTestCase, TestRepositoryLoaderRelationPassTwo
):
    # Run all tests in TestRepositoryLoaderRelationPassTwo
    pass
