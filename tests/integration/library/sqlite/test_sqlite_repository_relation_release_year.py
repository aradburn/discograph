from tests.integration.library.database.test_repository_relation_release_year import (
    TestRepositoryRelationReleaseYear,
)
from tests.integration.library.sqlite.sqlite_repository_test_case import (
    SqliteRepositoryTestCase,
)


class TestSqliteRepositoryRelationReleaseYear(
    SqliteRepositoryTestCase, TestRepositoryRelationReleaseYear
):
    # Run all tests in TestRepositoryRelationReleaseYear
    pass
