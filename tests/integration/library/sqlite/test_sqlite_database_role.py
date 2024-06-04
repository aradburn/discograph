from tests.integration.library.database.test_database_role import TestDatabaseRole
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteDatabaseRole(SqliteDatabaseTestCase, TestDatabaseRole):
    # Run all tests in TestDatabaseRole
    pass
