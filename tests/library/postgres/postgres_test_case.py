import unittest

from discograph import helpers
from discograph.config import PostgresTestConfiguration


# noinspection PyPep8Naming
def setUpModule():
    print("setup temp postgres DB", flush=True)
    helpers.setup_database(vars(PostgresTestConfiguration))


# noinspection PyPep8Naming
def tearDownModule():
    # release resources
    print("cleanup temp postgres DB", flush=True)
    helpers.shutdown_database()


class PostgresTestCase(unittest.TestCase):
    pass
