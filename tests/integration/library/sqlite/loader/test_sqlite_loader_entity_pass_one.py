from tests.integration.library.loader.test_loader_entity_pass_one import (
    TestLoaderEntityPassOne,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderEntityPassOne(SqliteRepositoryTestCase, TestLoaderEntityPassOne):
    # Run all tests in TestLoaderEntityPassOne
    pass
