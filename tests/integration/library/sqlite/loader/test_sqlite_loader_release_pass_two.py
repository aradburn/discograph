from tests.integration.library.loader.test_loader_release_pass_two import (
    TestLoaderReleasePassTwo,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteLoaderReleasePassTwo(SqliteDatabaseTestCase, TestLoaderReleasePassTwo):
    # Run all tests in TestLoaderReleasePassTwo
    pass
