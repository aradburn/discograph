from tests.integration.library.database.test_database_relation_from_release import (
    TestDatabaseRelationFromRelease,
)
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase


class TestPostgresRelationFromRelease(
    PostgresTestCase, TestDatabaseRelationFromRelease
):
    # Run all tests in TestDatabaseRelationFromRelease
    pass
