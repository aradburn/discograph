from tests.integration.library.loader.test_repository_loader_entity_pass_two import (
    TestRepositoryLoaderEntityPassTwo,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderEntityPassTwo(
    SqliteRepositoryTestCase, TestRepositoryLoaderEntityPassTwo
):
    # Run all tests in TestRepositoryLoaderEntityPassTwo
    pass
