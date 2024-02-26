from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase
from tests.integration.library.database.test_database_relation_from_release import (
    TestDatabaseRelationFromRelease,
)


class TestCockroachRelationFromRelease(
    CockroachTestCase, TestDatabaseRelationFromRelease
):
    # Run all tests in TestDatabaseRelationFromRelease
    pass
