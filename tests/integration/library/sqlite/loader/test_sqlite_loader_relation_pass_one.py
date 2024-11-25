from tests.integration.library.loader.test_loader_relation_pass_one import (
    TestLoaderRelationPassOne,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteLoaderRelationPassOne(
    SqliteDatabaseTestCase, TestLoaderRelationPassOne
):
    # Run all tests in TestLoaderRelationPassOne
    pass
