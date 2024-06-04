from tests.integration.library.loader.test_repository_loader_entity_pass_two import (
    TestRepositoryLoaderEntityPassTwo,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderEntityPassTwo(
    PostgresRepositoryTestCase, TestRepositoryLoaderEntityPassTwo
):
    # Run all tests in TestRepositoryLoaderEntityPassTwo
    pass
