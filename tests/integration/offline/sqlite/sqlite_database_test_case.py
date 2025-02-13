import logging

from discograph.config import SqliteTestConfiguration
from tests.integration.offline.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class SqliteDatabaseTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = SqliteTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
