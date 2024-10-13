from tests.integration.library.loader.test_loader_entity_pass_three import (
    TestLoaderEntityPassThree,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderEntityPassThree(
    PostgresRepositoryTestCase, TestLoaderEntityPassThree
):
    # Run all tests in TestLoaderEntityPassThree
    pass
