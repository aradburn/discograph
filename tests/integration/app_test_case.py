import logging
import unittest

from discograph import database
from discograph.app import setup_application, shutdown_application
from discograph.cache_manager import setup_cache, shutdown_cache
from discograph.config import PostgresTestConfiguration
from discograph.logging import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


class AppTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)
        log.info("setup temp postgres DB")
        # Note this currently has to be postgres because of string searches
        config = vars(PostgresTestConfiguration)
        setup_cache(config)
        database.setup_database(config)
        setup_application()

    @classmethod
    def tearDownClass(cls):
        # release resources
        log.info("cleanup temp postgres DB")
        shutdown_application()
        database.shutdown_database()
        shutdown_cache()
        shutdown_logging()
