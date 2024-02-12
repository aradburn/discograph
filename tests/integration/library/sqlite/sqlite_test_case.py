import logging
import unittest

from discograph import database
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.config import SqliteTestConfiguration
from discograph.logging import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


class SqliteTestCase(unittest.TestCase):
    def setUp(self):
        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")

    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)
        log.debug("setup temp sqlite DB")
        config = vars(SqliteTestConfiguration)
        setup_cache(config)
        database.setup_database(config)

    @classmethod
    def tearDownClass(cls):
        log.debug("cleanup temp sqlite DB")
        database.shutdown_database()
        shutdown_cache()
        shutdown_logging()
