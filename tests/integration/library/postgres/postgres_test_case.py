import logging

from discograph.config import PostgresTestConfiguration
from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_relation_grapher import (
    PostgresRelationGrapher,
)
from discograph.library.postgres.postgres_release import PostgresRelease
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class PostgresTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = PostgresTestConfiguration()
        DatabaseTestCase.entity = PostgresEntity
        DatabaseTestCase.relation = PostgresRelation
        DatabaseTestCase.release = PostgresRelease
        DatabaseTestCase.relation_grapher = PostgresRelationGrapher
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
