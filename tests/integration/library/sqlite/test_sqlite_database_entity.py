from tests.integration.library.database.test_database_entity import TestDatabaseEntity
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteDatabaseEntity(SqliteDatabaseTestCase, TestDatabaseEntity):
    # Run all tests in TestDatabaseEntity
    pass
