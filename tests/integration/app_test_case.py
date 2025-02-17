import logging

from discograph.app.app import shutdown_application, create_app
from discograph.config import (
    SqliteOfflineTestConfiguration,
    ALL_RUNTIME_DATABASE_TABLE_NAMES,
    PostgresOfflineTestConfiguration,
)
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

        _config = SqliteOfflineTestConfiguration()
        # _config = PostgresTestConfiguration()
        _app = create_app(_config)
        _app.config.update(
            {
                "TESTING": True,
            }
        )

        # For testing, drop and recreate all tables
        RuntimeDatabaseManager.runtime_db_helper.drop_tables(
            ALL_RUNTIME_DATABASE_TABLE_NAMES
        )
        RuntimeDatabaseManager.runtime_db_helper.create_tables(
            ALL_RUNTIME_DATABASE_TABLE_NAMES
        )

        TransferManager.transfer_all()
        RuntimeDatabaseManager.runtime_db_helper.load_tables()

        cls.client = _app.test_client()

    @classmethod
    def tearDownClass(cls):
        shutdown_application()
        super().tearDownClass()


# @classmethod
# def setUpClass(cls):
#     DatabaseTestCase._config = PostgresTestConfiguration()
#     super().setUpClass()
#     create_app(DatabaseTestCase._config)
