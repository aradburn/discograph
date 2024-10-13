from tests.integration.library.data_access_layer.test_role_data_access import (
    TestRoleDataAccess,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteRoleDataAccess(SqliteDatabaseTestCase, TestRoleDataAccess):
    # Run all tests in TestRoleDataAccess
    pass
