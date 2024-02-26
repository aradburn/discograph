import logging

from discograph import database
from discograph.app import setup_application, shutdown_application
from discograph.config import PostgresTestConfiguration
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_relation_grapher import (
    PostgresRelationGrapher,
)
from discograph.library.postgres.postgres_release import PostgresRelease
from discograph.logging_config import setup_logging, shutdown_logging
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class AppTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = PostgresTestConfiguration()
        DatabaseTestCase.entity = PostgresEntity
        DatabaseTestCase.relation = PostgresRelation
        DatabaseTestCase.release = PostgresRelease
        DatabaseTestCase.relation_grapher = PostgresRelationGrapher
        super().setUpClass()
        setup_application()

    @classmethod
    def tearDownClass(cls):
        shutdown_application()
        super().tearDownClass()
