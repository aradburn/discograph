import logging

from discograph.config import SqliteOfflineTestConfiguration
from tests.integration.offline.database.offline_repository_test_case import (
    OfflineRepositoryTestCase,
)

log = logging.getLogger(__name__)


class SqliteRepositoryTestCase(OfflineRepositoryTestCase):
    @classmethod
    def setUpClass(cls):
        OfflineRepositoryTestCase._offline_config = SqliteOfflineTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
