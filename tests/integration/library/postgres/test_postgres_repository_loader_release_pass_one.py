from tests.integration.library.loader.test_repository_loader_release_pass_one import (
    TestRepositoryLoaderReleasePassOne,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderReleasePassOne(
    PostgresRepositoryTestCase, TestRepositoryLoaderReleasePassOne
):
    # Run all tests in TestRepositoryLoaderReleasePassOne
    pass
