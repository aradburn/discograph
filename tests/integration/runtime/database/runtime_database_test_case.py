import logging

from sqlalchemy.exc import DatabaseError

from discograph.config import (
    Configuration,
)
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
from discograph.transfer.transfer_manager import TransferManager
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)

log = logging.getLogger(__name__)


class RuntimeDatabaseTestCase(OfflineDatabaseTestCase):
    _runtime_config: Configuration = None

    # noinspection PyPep8Naming
    def __init__(self, methodName="runTest"):
        ignore_test_prefixes = (
            "TestRuntimeDatabase",
            "TestRuntimeEntity",
            "TestRuntimeRelation",
            "TestRuntimeRole",
        )
        if self.__class__.__name__.startswith(ignore_test_prefixes):
            # don't run these tests in the abstract base implementation
            methodName = "runTestIgnoreInBaseClass"
            # methodName = "runNoTestsInBaseClass"
        super().__init__(methodName)

    def runTestIgnoreInBaseClass(self):
        pass

    @classmethod
    def setUpClass(cls):
        print("RuntimeDatabaseTestCase setUpClass")
        super().setUpClass()

        log.debug("RuntimeDatabaseTestCase setUpClass")

        if RuntimeDatabaseTestCase._runtime_config is not None:
            try:
                RuntimeDatabaseManager.setup_database(cls._runtime_config)
            except DatabaseError:
                log.error("Error in runtime database setup")
            else:
                TransferManager.transfer_all()
                RuntimeDatabaseManager.runtime_database_helper.load_tables()

    @classmethod
    def tearDownClass(cls):
        log.info(f"RuntimeDatabaseTestCase tearDownClass: {cls.__name__}")
        # release resources
        if RuntimeDatabaseTestCase._runtime_config is not None:
            RuntimeDatabaseManager.shutdown_database()
        super().tearDownClass()

    # def setUp(self):
    #     log.info("-------------------------------------------------------------------")
    #     log.info(f"Test {self.id()}")
