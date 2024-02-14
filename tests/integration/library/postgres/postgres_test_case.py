import atexit
import logging
import unittest

from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.config import PostgresTestConfiguration
from discograph.database import shutdown_database, setup_database
from discograph.logging_config import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


class PostgresTestCase(unittest.TestCase):
    def setUp(self):
        log.info("-------------------------------------------------------------------")
        log.info(f"Test {self.id()}")

    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)
        log.debug("setup temp postgres DB")
        config = vars(PostgresTestConfiguration)
        setup_cache(config)
        setup_database(config)
        atexit.register(shutdown_database)

    @classmethod
    def tearDownClass(cls):
        log.debug("cleanup temp postgres DB")
        shutdown_database()
        shutdown_cache()
        shutdown_logging()
