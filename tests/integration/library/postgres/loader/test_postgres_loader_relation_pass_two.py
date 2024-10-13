from tests.integration.library.loader.test_loader_relation_pass_two import (
    TestLoaderRelationPassTwo,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderRelationPassTwo(
    PostgresRepositoryTestCase, TestLoaderRelationPassTwo
):
    # Run all tests in TestLoaderRelationPassTwo
    pass
