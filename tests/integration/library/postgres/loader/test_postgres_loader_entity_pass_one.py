from tests.integration.library.loader.test_loader_entity_pass_one import (
    TestLoaderEntityPassOne,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresLoaderEntityPassOne(
    PostgresDatabaseTestCase, TestLoaderEntityPassOne
):
    # Run all tests in TestLoaderEntityPassOne
    pass
