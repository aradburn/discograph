from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase
from tests.integration.library.database.test_database_relation import (
    TestDatabaseRelation,
)


class TestCockroachRelationFromRelease(CockroachTestCase, TestDatabaseRelation):
    # Run all tests in TestDatabaseRelation
    pass
