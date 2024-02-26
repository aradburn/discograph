from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase
from tests.integration.library.database.test_database_release import TestDatabaseRelease


class TestCockroachRelease(CockroachTestCase, TestDatabaseRelease):
    # Run all tests in TestDatabaseRelease
    pass
