import unittest

from discograph import helpers
from discograph.app import setup_application, shutdown_application
from discograph.config import PostgresTestConfiguration


# noinspection PyPep8Naming
def setup_module():
    print("setup temp postgres DB", flush=True)
    setup_application()
    helpers.setup_database(vars(PostgresTestConfiguration))


# noinspection PyPep8Naming
def teardown_module():
    # release resources
    print("cleanup temp postgres DB", flush=True)
    helpers.shutdown_database()
    shutdown_application()


class AppTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_module()

    @classmethod
    def tearDownClass(cls):
        teardown_module()
