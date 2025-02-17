import logging

from discograph.config import PostgresOfflineTestConfiguration
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)

log = logging.getLogger(__name__)


class PostgresDatabaseTestCase(OfflineDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        OfflineDatabaseTestCase._offline_config = PostgresOfflineTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
