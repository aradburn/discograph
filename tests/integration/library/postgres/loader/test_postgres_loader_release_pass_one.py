from tests.integration.library.loader.test_loader_release_pass_one import (
    TestLoaderReleasePassOne,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresLoaderReleasePassOne(
    PostgresRepositoryTestCase, TestLoaderReleasePassOne
):
    # Run all tests in TestLoaderReleasePassOne
    pass
