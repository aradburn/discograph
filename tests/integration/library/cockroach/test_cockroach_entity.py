from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase
from tests.integration.library.database.test_database_entity import TestDatabaseEntity


class TestCockroachEntity(CockroachTestCase, TestDatabaseEntity):
    # Run all tests in TestDatabaseEntity
    pass
