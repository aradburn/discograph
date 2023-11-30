import unittest

from discograph import database
from discograph.config import PostgresTestConfiguration


# noinspection PyPep8Naming
# def setUpModule():
#     print("setup temp postgres DB", flush=True)
#     helpers.setup_database(vars(PostgresTestConfiguration))


# noinspection PyPep8Naming
# def tearDownModule():
# release resources
# print("cleanup temp postgres DB", flush=True)
# helpers.shutdown_database()


class PostgresTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("setup temp postgres DB", flush=True)
        database.setup_database(vars(PostgresTestConfiguration))

    @classmethod
    def tearDownClass(cls):
        print("cleanup temp postgres DB", flush=True)
        database.shutdown_database()
