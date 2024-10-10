from tests.integration.library.loader.test_loader_entity_pass_one import (
    TestLoaderEntityPassOne,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderEntityPassOne(
    PostgresRepositoryTestCase, TestLoaderEntityPassOne
):
    # Run all tests in TestLoaderEntityPassOne
    pass
