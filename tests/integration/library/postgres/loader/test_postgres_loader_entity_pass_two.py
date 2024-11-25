from tests.integration.library.loader.test_loader_entity_pass_two import (
    TestLoaderEntityPassTwo,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresLoaderEntityPassTwo(
    PostgresDatabaseTestCase, TestLoaderEntityPassTwo
):
    # Run all tests in TestLoaderEntityPassTwo
    pass
