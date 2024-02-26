from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase
from tests.integration.library.database.test_database_relation_grapher import (
    TestDatabaseRelationGrapher,
)


class TestCockroachRelationGrapher(CockroachTestCase, TestDatabaseRelationGrapher):
    # Run all tests in TestDatabaseRelationGrapher
    pass
