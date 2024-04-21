from tests.integration.library.loader.test_repository_loader_entity_pass_one import (
    TestRepositoryLoaderEntityPassOne,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderEntityPassOne(
    PostgresRepositoryTestCase, TestRepositoryLoaderEntityPassOne
):
    # Run all tests in TestRepositoryLoaderEntityPassOne
    pass
