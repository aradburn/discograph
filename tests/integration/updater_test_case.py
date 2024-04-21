import logging

from discograph.config import PostgresTestConfiguration
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class UpdaterTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        from discograph.library.database.database_helper import DatabaseHelper

        DatabaseTestCase._config = PostgresTestConfiguration()
        # DatabaseTestCase.entity = PostgresEntityDB
        # DatabaseTestCase.relation = PostgresRelationDB
        # DatabaseTestCase.release = PostgresReleaseDB
        DatabaseTestCase.relation_grapher = RelationGrapher
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
