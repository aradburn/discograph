from tests.integration.library.loader.test_repository_loader_entity_pass_one import (
    TestRepositoryLoaderEntityPassOne,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderEntityPassOne(
    SqliteRepositoryTestCase, TestRepositoryLoaderEntityPassOne
):
    # Run all tests in TestRepositoryLoaderEntityPassOne
    pass
