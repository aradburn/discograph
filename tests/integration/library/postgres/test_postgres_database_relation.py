from tests.integration.library.database.test_database_relation import (
    TestDatabaseRelation,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresDatabaseRelation(PostgresDatabaseTestCase, TestDatabaseRelation):
    # Run all tests in TestDatabaseRelation
    pass
