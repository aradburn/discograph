from tests.integration.library.database.test_repository_role import TestRepositoryRole
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryRole(PostgresRepositoryTestCase, TestRepositoryRole):
    # Run all tests in TestRepositoryRole
    pass
