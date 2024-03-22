from tests.integration.library.database.test_database_role import TestDatabaseRole
from tests.integration.library.sqlite.sqlite_test_case import SqliteTestCase


class TestSqliteRole(SqliteTestCase, TestDatabaseRole):
    # Run all tests in TestDatabaseRole
    pass
