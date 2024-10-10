from tests.integration.library.loader.test_loader_release_pass_two import (
    TestLoaderReleasePassTwo,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderReleasePassTwo(
    SqliteRepositoryTestCase, TestLoaderReleasePassTwo
):
    # Run all tests in TestLoaderReleasePassTwo
    pass
