from tests.integration.library.database.test_database_entity import (
    TestDatabaseEntity,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresDatabaseEntity(PostgresDatabaseTestCase, TestDatabaseEntity):
    # Run all tests in TestDatabaseEntity
    pass
