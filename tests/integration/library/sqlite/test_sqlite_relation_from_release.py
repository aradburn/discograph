from tests.integration.library.database.test_database_relation_from_release import (
    TestDatabaseRelationFromRelease,
)
from tests.integration.library.sqlite.sqlite_test_case import SqliteTestCase


class TestSqliteRelationFromRelease(SqliteTestCase, TestDatabaseRelationFromRelease):
    # Run all tests in TestDatabaseRelationFromRelease
    pass
