from tests.integration.library.loader.test_repository_loader_relation_pass_one import (
    TestRepositoryLoaderRelationPassOne,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderRelationPassOne(
    PostgresRepositoryTestCase, TestRepositoryLoaderRelationPassOne
):
    # Run all tests in TestRepositoryLoaderRelationPassOne
    pass
