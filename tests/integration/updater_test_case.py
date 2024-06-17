import logging

from discograph.config import PostgresTestConfiguration, TEST_DATA_DIR
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class UpdaterTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = PostgresTestConfiguration()
        DatabaseTestCase.relation_grapher = RelationGrapher
        super().setUpClass()

        # Run the test update process
        DatabaseHelper.db_helper.load_tables(TEST_DATA_DIR, "testupdate")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def setUp(self):
        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")
