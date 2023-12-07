import unittest

from discograph import database
from discograph.config import CockroachTestConfiguration


# noinspection PyPep8Naming
def setUpModule():
    print("setup temp Cockroach DB", flush=True)
    database.setup_database(vars(CockroachTestConfiguration))


# noinspection PyPep8Naming
def tearDownModule():
    # release resources
    print("cleanup temp Cockroach DB", flush=True)
    database.shutdown_database()


class CockroachTestCase(unittest.TestCase):
    pass
