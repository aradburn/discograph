import unittest

from discograph import helpers
from discograph.config import SqliteTestConfiguration


# noinspection PyPep8Naming
def setUpModule():
    print("setup temp sqlite DB", flush=True)
    helpers.setup_database(vars(SqliteTestConfiguration))


# noinspection PyPep8Naming
def tearDownModule():
    # release resources
    print("cleanup temp sqlite DB", flush=True)
    helpers.shutdown_database()


class SqliteTestCase(unittest.TestCase):
    pass
