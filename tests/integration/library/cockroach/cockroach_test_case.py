import logging
import unittest
from unittest import SkipTest

from discograph import database
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.config import CockroachTestConfiguration
from discograph.logging import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


class CockroachTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)
        log.debug("setup temp Cockroach DB")
        config = vars(CockroachTestConfiguration)
        try:
            database.setup_database(config)
        except Exception as e:
            raise SkipTest("Cannot connect to Cockroach Database")
        setup_cache(config)

    @classmethod
    def tearDownClass(cls):
        log.debug("cleanup temp Cockroach DB")
        database.shutdown_database()
        shutdown_cache()
        shutdown_logging()
