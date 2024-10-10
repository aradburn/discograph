from tests.integration.library.loader.test_loader_relation_pass_one import (
    TestLoaderRelationPassOne,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderRelationPassOne(
    PostgresRepositoryTestCase, TestLoaderRelationPassOne
):
    # Run all tests in TestLoaderRelationPassOne
    pass
