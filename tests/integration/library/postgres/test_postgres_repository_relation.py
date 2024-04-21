from tests.integration.library.database.test_repository_relation import (
    TestRepositoryRelation,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryRelation(
    PostgresRepositoryTestCase, TestRepositoryRelation
):
    # Run all tests in TestRepositoryRelation
    pass
