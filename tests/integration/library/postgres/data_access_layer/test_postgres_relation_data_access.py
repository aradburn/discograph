from tests.integration.library.data_access_layer.test_relation_data_access import (
    TestRelationDataAccess,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresRelationDataAccess(PostgresDatabaseTestCase, TestRelationDataAccess):
    # Run all tests in TestRelationDataAccess
    pass
