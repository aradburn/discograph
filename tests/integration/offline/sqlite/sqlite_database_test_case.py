import logging

from discograph.config import SqliteOfflineTestConfiguration
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)

log = logging.getLogger(__name__)


class SqliteDatabaseTestCase(OfflineDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        OfflineDatabaseTestCase._offline_config = SqliteOfflineTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
