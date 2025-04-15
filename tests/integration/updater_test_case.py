import logging

from discograph.config import PostgresOfflineTestConfiguration, TEST_DATA_DIR
from discograph.offline.offline_database_manager import OfflineDatabaseManager
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)

log = logging.getLogger(__name__)


class UpdaterTestCase(OfflineDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        OfflineDatabaseTestCase._offline_config = PostgresOfflineTestConfiguration()
        super().setUpClass()

        # Run the test update process
        OfflineDatabaseManager.offline_database_helper.load_tables(
            TEST_DATA_DIR, "testupdate", is_bulk_inserts=False
        )

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def setUp(self):
        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")
