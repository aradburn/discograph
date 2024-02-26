from tests.integration.library.database.test_database_entity import TestDatabaseEntity
from tests.integration.library.sqlite.sqlite_test_case import SqliteTestCase


class TestSqliteEntity(SqliteTestCase, TestDatabaseEntity):
    # Run all tests in TestDatabaseEntity
    pass
