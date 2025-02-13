import logging
import unittest

from sqlalchemy.exc import DatabaseError

from discograph.config import (
    Configuration,
    TEST_DATA_DIR,
    ALL_OFFLINE_DATABASE_TABLE_NAMES,
    OFFLINE_DATABASE_TABLE_NAMES_WITHOUT_ROLE,
    TEXT_SEARCH_PATH,
)
from discograph.library.cache.cache_manager import CacheManager
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.logging_config import setup_logging, shutdown_logging
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


class DatabaseTestCase(unittest.TestCase):
    _config: Configuration = None

    # noinspection PyPep8Naming
    def __init__(self, methodName="runTest"):
        ignore_test_prefixes = (
            "TestDatabase",
            "TestEntity",
            "TestRelease",
            "TestRelation",
            "TestRole",
            "TestLoader",
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
        setup_logging(is_testing=True)
        # log.info(f"DatabaseTestCase setUpClass: {cls.__name__}")
        # log.info(f"DatabaseTestCase _config: {cls._config}")
        if cls._config is not None:
            CacheManager.setup_cache(cls._config)
            try:
                OfflineDatabaseManager.setup_database(cls._config)
            except DatabaseError:
                log.error("Error in database setup")
            else:
                # db_logger = logging.getLogger("peewee")
                # db_logger.setLevel(logging.DEBUG)
                OfflineDatabaseManager.db_helper.drop_tables(
                    OFFLINE_DATABASE_TABLE_NAMES_WITHOUT_ROLE
                )
                OfflineDatabaseManager.db_helper.create_tables(
                    ALL_OFFLINE_DATABASE_TABLE_NAMES
                )
                # LoaderRole.load_roles_into_database()
                OfflineDatabaseManager.db_helper.load_tables(
                    TEST_DATA_DIR, "testinsert", is_bulk_inserts=True
                )
                OfflineDatabaseManager.db_helper.text_search_index = (
                    TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
                )
                print("Done setup")

    @classmethod
    def tearDownClass(cls):
        log.info(f"DatabaseTestCase tearDownClass: {cls.__name__}")
        # release resources
        if cls._config is not None:
            OfflineDatabaseManager.shutdown_database()
            CacheManager.shutdown_cache()
            shutdown_logging()

    def setUp(self):
        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")
