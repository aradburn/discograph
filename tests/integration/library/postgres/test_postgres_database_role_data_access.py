from tests.integration.library.data_access_layer.test_role_data_access import (
    TestDatabaseRoleDataAccess,
)
from tests.integration.library.postgres.postgres_database_test_case import (
    PostgresDatabaseTestCase,
)


class TestPostgresDatabaseRoleDataAccess(
    PostgresDatabaseTestCase, TestDatabaseRoleDataAccess
):
    # Run all tests in TestDatabaseRoleDataAccess
    pass
