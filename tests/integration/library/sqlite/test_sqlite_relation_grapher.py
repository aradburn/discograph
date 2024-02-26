from tests.integration.library.database.test_database_relation_grapher import (
    TestDatabaseRelationGrapher,
)
from tests.integration.library.sqlite.sqlite_test_case import SqliteTestCase


class TestSqliteRelationGrapher(SqliteTestCase, TestDatabaseRelationGrapher):
    # Run all tests in TestDatabaseRelationGrapher
    pass
