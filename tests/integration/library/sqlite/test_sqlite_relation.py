from tests.integration.library.database.test_database_relation import (
    TestDatabaseRelation,
)
from tests.integration.library.sqlite.sqlite_test_case import SqliteTestCase


class TestSqliteRelation(SqliteTestCase, TestDatabaseRelation):
    # Run all tests in TestDatabaseRelation
    pass
