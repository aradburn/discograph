from tests.integration.library.database.test_repository_metadata import (
    TestRepositoryMetadata,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryMetadata(
    PostgresRepositoryTestCase, TestRepositoryMetadata
):
    # Run all tests in TestRepositoryMetadata
    pass
