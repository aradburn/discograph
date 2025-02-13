import logging

from discograph.config import PostgresTestConfiguration
from tests.integration.offline.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class PostgresDatabaseTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = PostgresTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
