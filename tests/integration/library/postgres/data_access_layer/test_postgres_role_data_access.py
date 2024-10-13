from tests.integration.library.data_access_layer.test_role_data_access import (
    TestRoleDataAccess,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresRoleDataAccess(PostgresDatabaseTestCase, TestRoleDataAccess):
    # Run all tests in TestRoleDataAccess
    pass
