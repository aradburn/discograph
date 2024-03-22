from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase
from tests.integration.library.database.test_database_role import TestDatabaseRole


class TestCockroachRole(CockroachTestCase, TestDatabaseRole):
    # Run all tests in TestDatabaseRole
    pass
