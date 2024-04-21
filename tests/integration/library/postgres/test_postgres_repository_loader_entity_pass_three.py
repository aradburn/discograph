from tests.integration.library.loader.test_repository_loader_entity_pass_three import (
    TestRepositoryLoaderEntityPassThree,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderEntityPassThree(
    PostgresRepositoryTestCase, TestRepositoryLoaderEntityPassThree
):
    # Run all tests in TestRepositoryLoaderEntityPassThree
    pass
