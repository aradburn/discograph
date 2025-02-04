from tests.integration.library.loader.test_loader_release_pass_two import (
    TestLoaderReleasePassTwo,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresLoaderReleasePassTwo(
    PostgresDatabaseTestCase, TestLoaderReleasePassTwo
):
    # Run all tests in TestLoaderReleasePassTwo
    pass
