from tests.integration.library.loader.test_repository_loader_relation_pass_two import (
    TestRepositoryLoaderRelationPassTwo,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderRelationPassTwo(
    PostgresRepositoryTestCase, TestRepositoryLoaderRelationPassTwo
):
    # Run all tests in TestRepositoryLoaderRelationPassTwo
    pass
