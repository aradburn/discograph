from tests.integration.library.loader.test_repository_loader_release_pass_one import (
    TestRepositoryLoaderReleasePassOne,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryLoaderReleasePassOne(
    SqliteRepositoryTestCase, TestRepositoryLoaderReleasePassOne
):
    # Run all tests in TestRepositoryLoaderReleasePassOne
    pass
