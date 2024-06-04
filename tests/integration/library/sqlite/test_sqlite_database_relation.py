from tests.integration.library.database.test_database_relation import (
    TestDatabaseRelation,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteDatabaseRelation(SqliteDatabaseTestCase, TestDatabaseRelation):
    # Run all tests in TestDatabaseRelation
    pass
