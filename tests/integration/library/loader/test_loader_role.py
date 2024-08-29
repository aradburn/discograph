import unittest

from discograph.library.loader.loader_role import LoaderRole
from discograph.logging_config import setup_logging


class TestLoaderRole(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)

    def test_load_instruments(self):
        # GIVEN
        num_instruments = LoaderRole.load_wikipedia_instruments()
        self.assertTrue(num_instruments > 0)
