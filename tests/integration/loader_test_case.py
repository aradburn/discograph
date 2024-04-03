import logging

from discograph.config import PostgresTestConfiguration
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_relation_grapher import (
    PostgresRelationGrapher,
)
from discograph.library.postgres.postgres_release import PostgresRelease
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class LoaderTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        from discograph.library.database.database_helper import DatabaseHelper

        DatabaseTestCase._config = PostgresTestConfiguration()
        DatabaseTestCase.entity = PostgresEntity
        DatabaseTestCase.relation = PostgresRelation
        DatabaseTestCase.release = PostgresRelease
        DatabaseTestCase.relation_grapher = PostgresRelationGrapher
        super().setUpClass()

        # Run the test update process
        DatabaseHelper.db_helper.update_tables("testupdate")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def setUp(self):
        self.test_session = DatabaseHelper.session_factory

        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")
