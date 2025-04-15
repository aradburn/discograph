import logging

from discograph.config import (
    PostgresOfflineTestConfiguration,
    PostgresRuntimeTestConfiguration,
)
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)
from tests.integration.runtime.database.runtime_database_test_case import (
    RuntimeDatabaseTestCase,
)

log = logging.getLogger(__name__)


class PostgresRuntimeDatabaseTestCase(RuntimeDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        print("PostgresRuntimeDatabaseTestCase setUpClass")
        RuntimeDatabaseTestCase._runtime_config = PostgresRuntimeTestConfiguration()
        OfflineDatabaseTestCase._offline_config = PostgresOfflineTestConfiguration()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
