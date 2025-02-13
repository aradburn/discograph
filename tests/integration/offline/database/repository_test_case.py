import logging
import unittest

from sqlalchemy.exc import DatabaseError

from discograph.config import (
    Configuration,
    ALL_OFFLINE_DATABASE_TABLE_NAMES,
    OFFLINE_DATABASE_TABLE_NAMES_WITHOUT_ROLE,
    TEXT_SEARCH_PATH,
)
from discograph.library.cache.cache_manager import CacheManager
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.logging_config import setup_logging, shutdown_logging
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


class RepositoryTestCase(unittest.TestCase):
    _config: Configuration = None

    # noinspection PyPep8Naming
    def __init__(self, methodName="runTest"):
        ignore_test_prefixes = ("TestRepository", "TestLoader")
        if self.__class__.__name__.startswith(ignore_test_prefixes):
            # don't run these tests in the abstract base implementation
            methodName = "runTestIgnoreInBaseClass"
            # methodName = "runNoTestsInBaseClass"
        super().__init__(methodName)

    def runTestIgnoreInBaseClass(self):
        pass

    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)
        if cls._config is not None:
            CacheManager.setup_cache(cls._config)
            try:
                OfflineDatabaseManager.setup_database(cls._config)
            except DatabaseError:
                log.error("Error in database setup")
                OfflineDatabaseManager.db_helper.drop_tables(
                    ALL_OFFLINE_DATABASE_TABLE_NAMES
                )
            else:
                OfflineDatabaseManager.db_helper.drop_tables(
                    OFFLINE_DATABASE_TABLE_NAMES_WITHOUT_ROLE
                )
                OfflineDatabaseManager.db_helper.create_tables(
                    ALL_OFFLINE_DATABASE_TABLE_NAMES
                )
                # LoaderRole.load_roles_into_database()
                # Note: No data loading, empty repositories
                OfflineDatabaseManager.db_helper.text_search_index = (
                    TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
                )

    @classmethod
    def tearDownClass(cls):
        log.info(f"RepositoryTestCase tearDownClass: {cls.__name__}")
        # release resources
        if cls._config is not None:
            OfflineDatabaseManager.shutdown_database()
            CacheManager.shutdown_cache()
            shutdown_logging()

    @classmethod
    def resetDB(cls):
        if OfflineDatabaseManager.db_helper is not None:
            log.info(f"Reset database tables: {cls.__name__}")
            OfflineDatabaseManager.db_helper.drop_tables(
                ALL_OFFLINE_DATABASE_TABLE_NAMES
            )
            OfflineDatabaseManager.db_helper.create_tables(
                ALL_OFFLINE_DATABASE_TABLE_NAMES
            )

    def setUp(self):
        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")
