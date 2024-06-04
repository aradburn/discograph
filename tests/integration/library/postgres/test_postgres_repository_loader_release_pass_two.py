from tests.integration.library.loader.test_repository_loader_release_pass_two import (
    TestRepositoryLoaderReleasePassTwo,
)
from tests.integration.library.postgres.postgres_repository_test_case import (
    PostgresRepositoryTestCase,
)


class TestPostgresRepositoryLoaderReleasePassTwo(
    PostgresRepositoryTestCase, TestRepositoryLoaderReleasePassTwo
):
    # Run all tests in TestRepositoryLoaderReleasePassTwo
    pass
