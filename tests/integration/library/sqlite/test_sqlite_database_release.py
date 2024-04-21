from tests.integration.library.database.test_database_release import (
    TestDatabaseRelease,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteDatabaseRelease(SqliteDatabaseTestCase, TestDatabaseRelease):
    # Run all tests in TestDatabaseRelease
    pass
