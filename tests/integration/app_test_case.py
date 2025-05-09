import logging

from discograph.app.fastapi_app import create_app
from discograph.config import (
    ALL_RUNTIME_DATABASE_TABLE_NAMES,
    PostgresOfflineTestConfiguration,
    SqliteRuntimeTestConfiguration,
)
from discograph.offline.offline_database_manager import OfflineDatabaseManager
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
from discograph.transfer.transfer_manager import TransferManager
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)

log = logging.getLogger(__name__)


class AppTestCase(OfflineDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        log.debug("AppTestCase setUpClass")

        OfflineDatabaseTestCase._offline_config = PostgresOfflineTestConfiguration()
        super().setUpClass()

        _runtime_config = SqliteRuntimeTestConfiguration()
        _app = create_app(_runtime_config)
        # _app.config.update(
        #     {
        #         "TESTING": True,
        #     }
        # )

        # For testing, drop and recreate all tables
        RuntimeDatabaseManager.runtime_database_helper.drop_tables(
            ALL_RUNTIME_DATABASE_TABLE_NAMES
        )
        RuntimeDatabaseManager.runtime_database_helper.create_tables(
            ALL_RUNTIME_DATABASE_TABLE_NAMES
        )

        TransferManager.transfer_all()
        RuntimeDatabaseManager.runtime_database_helper.load_tables()

        # cls.client = _app.test_client()

    @classmethod
    def tearDownClass(cls):
        if OfflineDatabaseTestCase._offline_config is not None:
            OfflineDatabaseManager.shutdown_database()
        # shutdown_application()
