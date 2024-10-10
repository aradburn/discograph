from tests.integration.library.loader.test_loader_role import (
    TestLoaderRole,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderRole(PostgresRepositoryTestCase, TestLoaderRole):
    # Run all tests in TestLoaderRole
    pass
