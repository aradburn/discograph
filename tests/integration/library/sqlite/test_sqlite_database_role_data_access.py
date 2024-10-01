from tests.integration.library.data_access_layer.test_role_data_access import (
    TestDatabaseRoleDataAccess,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteDatabaseRoleDataAccess(
    SqliteDatabaseTestCase, TestDatabaseRoleDataAccess
):
    # Run all tests in TestDatabaseRoleDataAccess
    pass
