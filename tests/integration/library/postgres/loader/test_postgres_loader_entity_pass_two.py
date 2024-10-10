from tests.integration.library.loader.test_loader_entity_pass_two import (
    TestLoaderEntityPassTwo,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderEntityPassTwo(
    PostgresRepositoryTestCase, TestLoaderEntityPassTwo
):
    # Run all tests in TestLoaderEntityPassTwo
    pass
