from tests.integration.library.data_access_layer.test_entity_data_access import (
    TestEntityDataAccess,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresEntityDataAccess(PostgresDatabaseTestCase, TestEntityDataAccess):
    # Run all tests in TestEntityDataAccess
    pass
