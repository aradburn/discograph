import logging

from discograph.config import (
    SqliteOfflineTestConfiguration,
    SqliteRuntimeTestConfiguration,
)
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)
from tests.integration.runtime.database.runtime_database_test_case import (
    RuntimeDatabaseTestCase,
)

log = logging.getLogger(__name__)


class SqliteRuntimeDatabaseTestCase(RuntimeDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        RuntimeDatabaseTestCase._runtime_config = SqliteRuntimeTestConfiguration()
        OfflineDatabaseTestCase._offline_config = SqliteOfflineTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
