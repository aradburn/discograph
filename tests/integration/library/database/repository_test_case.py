import logging
import unittest
from typing import Type

from sqlalchemy.exc import DatabaseError

from discograph import database
from discograph.config import Configuration, ALL_DATABASE_TABLE_NAMES
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.loader.loader_role import LoaderRole
from discograph.library.relation_grapher import RelationGrapher
from discograph.logging_config import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


class RepositoryTestCase(unittest.TestCase):
    _config: Configuration = None
    _db_helper: Type[DatabaseHelper] = None
    relation_grapher: Type[RelationGrapher] = None

    # noinspection PyPep8Naming
    def __init__(self, methodName="runTest"):
        if self.__class__.__name__.startswith("TestRepository"):
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
            setup_cache(cls._config)
            try:
                cls._db_helper = database.setup_database(cls._config)
            except DatabaseError:
                log.error("Error in database setup")
            else:
                cls._db_helper.drop_tables()
                cls._db_helper.create_tables(ALL_DATABASE_TABLE_NAMES)
                LoaderRole.load_roles_into_database()
                # Note: No data loading, empty repositories

    @classmethod
    def tearDownClass(cls):
        log.info(f"RepositoryTestCase tearDownClass: {cls.__name__}")
        # release resources
        if cls._config is not None:
            database.shutdown_database(cls._config)
            shutdown_cache()
            shutdown_logging()

    @classmethod
    def resetDB(cls):
        if cls._db_helper is not None:
            log.info(f"Reset database tables: {cls.__name__}")
            cls._db_helper.drop_tables()
            cls._db_helper.create_tables(ALL_DATABASE_TABLE_NAMES)

    def setUp(self):
        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")
