from tests.integration.library.loader.test_loader_entity_pass_three import (
    TestLoaderEntityPassThree,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderEntityPassThree(
    SqliteRepositoryTestCase, TestLoaderEntityPassThree
):
    # Run all tests in TestLoaderEntityPassThree
    pass
