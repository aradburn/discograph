from tests.integration.library.loader.test_loader_release_pass_two import (
    TestLoaderReleasePassTwo,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderReleasePassTwo(
    PostgresRepositoryTestCase, TestLoaderReleasePassTwo
):
    # Run all tests in TestLoaderReleasePassTwo
    pass
