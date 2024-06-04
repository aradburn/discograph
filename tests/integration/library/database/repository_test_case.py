import logging
import unittest
from typing import Type

from sqlalchemy.exc import DatabaseError

from discograph import database
from discograph.config import Configuration
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.relation_grapher import RelationGrapher
from discograph.logging_config import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


class RepositoryTestCase(unittest.TestCase):
    _config: Configuration = None
    _db_helper: DatabaseHelper = None
    relation_grapher: Type[RelationGrapher] = None

    # noinspection PyPep8Naming
    def __init__(self, methodName="runTest"):
        if self.__class__.__name__.startswith("TestRepository"):
            # don't run these tests in the abstract base implementation
            methodName = "runNoTestsInBaseClass"
        super().__init__(methodName)

    def runNoTestsInBaseClass(self):
        pass

    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)
        if cls._config is not None:
            setup_cache(cls._config)
            try:
                _db_helper = database.setup_database(cls._config)
            except DatabaseError:
                log.error("Error in database setup")
            else:
                _db_helper.drop_tables()
                _db_helper.create_tables()

    @classmethod
    def tearDownClass(cls):
        log.info(f"RepositoryTestCase tearDownClass: {cls.__name__}")
        # release resources
        if cls._config is not None:
            database.shutdown_database(cls._config)
            shutdown_cache()
            shutdown_logging()

    def setUp(self):
        # self.test_session = DatabaseHelper.session_factory

        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")
