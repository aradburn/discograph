from tests.integration.library.loader.test_repository_loader_role import (
    TestRepositoryLoaderRole,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderRole(
    PostgresRepositoryTestCase, TestRepositoryLoaderRole
):
    # Run all tests in TestRepositoryLoaderRole
    pass
