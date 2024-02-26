from tests.integration.library.database.test_database_release import TestDatabaseRelease
from tests.integration.library.sqlite.sqlite_test_case import SqliteTestCase


class TestSqliteRelease(SqliteTestCase, TestDatabaseRelease):
    # Run all tests in TestDatabaseRelease
    pass
