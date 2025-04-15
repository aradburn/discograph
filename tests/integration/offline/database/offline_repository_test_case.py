import logging

from sqlalchemy.exc import DatabaseError

from discograph.config import (
    ALL_OFFLINE_DATABASE_TABLE_NAMES,
)
from discograph.offline.offline_database_manager import OfflineDatabaseManager
from tests.integration.offline.database.offline_database_test_case import (
    OfflineDatabaseTestCase,
)

log = logging.getLogger(__name__)


class OfflineRepositoryTestCase(OfflineDatabaseTestCase):
    # _config: Configuration = None

    # noinspection PyPep8Naming
    # def __init__(self, methodName="runTest"):
    #     ignore_test_prefixes = ("TestRepository", "TestLoader")
    #     if self.__class__.__name__.startswith(ignore_test_prefixes):
    #         # don't run these tests in the abstract base implementation
    #         methodName = "runTestIgnoreInBaseClass"
    #         # methodName = "runNoTestsInBaseClass"
    #     super().__init__(methodName)
    #
    # def runTestIgnoreInBaseClass(self):
    #     pass

    @classmethod
    def setUpClass(cls):
        log.info(f"RepositoryTestCase setUpClass: {cls.__name__}")
        super().setUpClass()
        cls.resetDB()

    # @classmethod
    # def setUpClass(cls):
    #     setup_logging(is_testing=True)
    #     if cls._config is not None:
    #         CacheManager.setup_cache(cls._config)
    #         try:
    #             OfflineDatabaseManager.setup_database(cls._config)
    #         except DatabaseError:
    #             log.error("Error in database setup")
    #             OfflineDatabaseManager.db_helper.drop_tables(
    #                 ALL_OFFLINE_DATABASE_TABLE_NAMES
    #             )
    #         else:
    #             OfflineDatabaseManager.db_helper.drop_tables(
    #                 OFFLINE_DATABASE_TABLE_NAMES_WITHOUT_ROLE
    #             )
    #             OfflineDatabaseManager.db_helper.create_tables(
    #                 ALL_OFFLINE_DATABASE_TABLE_NAMES
    #             )
    #             # LoaderRole.load_roles_into_database()
    #             # Note: No data loading, empty repositories
    #             OfflineDatabaseManager.db_helper.text_search_index = (
    #                 TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
    #             )
    #
    # @classmethod
    # def tearDownClass(cls):
    #     log.info(f"RepositoryTestCase tearDownClass: {cls.__name__}")
    #     # release resources
    #     if cls._config is not None:
    #         OfflineDatabaseManager.shutdown_database()
    #         CacheManager.shutdown_cache()
    #         shutdown_logging()

    @classmethod
    def tearDownClass(cls):
        log.info(f"RepositoryTestCase tearDownClass: {cls.__name__}")
        # release resources
        super().tearDownClass()

    @classmethod
    def resetDB(cls):
        if OfflineDatabaseManager.offline_database_helper is not None:
            log.info(f"Reset offline database tables: {cls.__name__}")
            try:
                OfflineDatabaseManager.offline_database_helper.drop_tables(
                    ALL_OFFLINE_DATABASE_TABLE_NAMES
                )
                OfflineDatabaseManager.offline_database_helper.create_tables(
                    ALL_OFFLINE_DATABASE_TABLE_NAMES
                )
            except DatabaseError:
                log.error("Error in RepositoryTestCase database reset")

    # def setUp(self):
    #     log.info("-------------------------------------------------------------------")
    #     log.info(f"Test {self.id()}")
