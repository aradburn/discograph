from xml.etree import ElementTree

from discograph import utils
from discograph.library.entity_type import EntityType
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_release import PostgresRelease
from tests.integration.library.database.test_database_relation import (
    TestDatabaseRelation,
)
from tests.integration.library.database.test_database_relation_from_release import (
    TestDatabaseRelationFromRelease,
)
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase


class TestPostgresRelation(PostgresTestCase, TestDatabaseRelation):
    # Run all tests in TestDatabaseRelation
    pass
