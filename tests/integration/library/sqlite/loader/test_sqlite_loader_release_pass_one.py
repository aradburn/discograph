from tests.integration.library.loader.test_loader_release_pass_one import (
    TestLoaderReleasePassOne,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteLoaderReleasePassOne(
    SqliteRepositoryTestCase, TestLoaderReleasePassOne
):
    # Run all tests in TestLoaderReleasePassOne
    pass
