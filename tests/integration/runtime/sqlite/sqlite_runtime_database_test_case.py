import logging

from discograph.config import SqliteTestConfiguration
from tests.integration.runtime.database.runtime_database_test_case import (
    RuntimeDatabaseTestCase,
)

log = logging.getLogger(__name__)


class SqliteRuntimeDatabaseTestCase(RuntimeDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        RuntimeDatabaseTestCase._config = SqliteTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
