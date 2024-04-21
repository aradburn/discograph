from tests.integration.library.loader.test_repository_loader_entity_pass_three import (
    TestRepositoryLoaderEntityPassThree,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderEntityPassThree(
    SqliteRepositoryTestCase, TestRepositoryLoaderEntityPassThree
):
    # Run all tests in TestRepositoryLoaderEntityPassThree
    pass
