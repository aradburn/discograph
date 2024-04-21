from tests.integration.library.loader.test_repository_loader_relation import (
    TestRepositoryLoaderRelation,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderRelation(
    PostgresRepositoryTestCase, TestRepositoryLoaderRelation
):
    # Run all tests in TestRepositoryLoaderRelation
    pass
