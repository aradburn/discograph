from tests.integration.library.loader.test_repository_loader_release_pass_two import (
    TestRepositoryLoaderReleasePassTwo,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderReleasePassTwo(
    SqliteRepositoryTestCase, TestRepositoryLoaderReleasePassTwo
):
    # Run all tests in TestRepositoryLoaderReleasePassTwo
    pass
