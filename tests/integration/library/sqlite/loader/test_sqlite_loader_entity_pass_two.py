from tests.integration.library.loader.test_loader_entity_pass_two import (
    TestLoaderEntityPassTwo,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderEntityPassTwo(SqliteRepositoryTestCase, TestLoaderEntityPassTwo):
    # Run all tests in TestLoaderEntityPassTwo
    pass
