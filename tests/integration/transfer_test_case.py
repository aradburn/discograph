import logging

from discograph.config import (
    ALL_RUNTIME_DATABASE_TABLE_NAMES,
    SqliteRuntimeTestConfiguration,
    Configuration,
    SqliteOfflineTestConfiguration,
)
from discograph.exceptions import DatabaseError
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)

log = logging.getLogger(__name__)


class TransferTestCase(OfflineDatabaseTestCase):
    _runtime_config: Configuration = None

    @classmethod
    def setUpClass(cls):
        log.debug("TransferTestCase setUpClass")

        OfflineDatabaseTestCase._offline_config = SqliteOfflineTestConfiguration()
        super().setUpClass()

        TransferTestCase._runtime_config = SqliteRuntimeTestConfiguration()

        if TransferTestCase._runtime_config is not None:
            try:
                RuntimeDatabaseManager.setup_database(TransferTestCase._runtime_config)
            except DatabaseError:
                log.error("Error in runtime database setup")

        # For testing, drop and recreate all tables
        RuntimeDatabaseManager.runtime_database_helper.drop_tables(
            ALL_RUNTIME_DATABASE_TABLE_NAMES
        )
        RuntimeDatabaseManager.runtime_database_helper.create_tables(
            ALL_RUNTIME_DATABASE_TABLE_NAMES
        )

    @classmethod
    def tearDownClass(cls):
        if TransferTestCase._runtime_config is not None:
            RuntimeDatabaseManager.shutdown_database()
        super().tearDownClass()
