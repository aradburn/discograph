from tests.integration.library.database.test_database_relation import (
    TestDatabaseRelation,
)
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase


class TestPostgresRelation(PostgresTestCase, TestDatabaseRelation):
    # Run all tests in TestDatabaseRelation
    pass
