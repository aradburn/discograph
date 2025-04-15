import logging

from discograph.config import PostgresOfflineTestConfiguration
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)
from tests.integration.offline.database.offline_repository_test_case import (
    OfflineRepositoryTestCase,
)

log = logging.getLogger(__name__)


class PostgresRepositoryTestCase(OfflineRepositoryTestCase):
    @classmethod
    def setUpClass(cls):
        OfflineDatabaseTestCase._offline_config = PostgresOfflineTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
