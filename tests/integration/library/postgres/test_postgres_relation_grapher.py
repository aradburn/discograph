import logging
from tests.integration.library.database.test_database_relation_grapher import (
    TestDatabaseRelationGrapher,
)
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase

log = logging.getLogger(__name__)


class TestPostgresRelationGrapher(PostgresTestCase, TestDatabaseRelationGrapher):
    # Run all tests in TestDatabaseRelationGrapher
    pass
