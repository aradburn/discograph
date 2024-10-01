import unittest

from discograph.library.loader.loader_role import LoaderRole
from discograph.logging_config import setup_logging


class TestLoaderRole(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)

    def test_load_wikipedia_instruments(self):
        # GIVEN

        # WHEN
        wikipedia_instruments = LoaderRole.load_wikipedia_instruments()

        # THEN
        expected = 2185
        actual = len(wikipedia_instruments)
        self.assertEqual(expected, actual)

    def test_load_hornbostel_sachs_instruments(self):
        # GIVEN

        # WHEN
        hornbostel_sachs_instruments = LoaderRole.load_hornbostel_sachs_instruments()

        # THEN
        expected = 1865
        actual = len(hornbostel_sachs_instruments)
        self.assertEqual(expected, actual)

    def test_load_roles_from_files(self):
        # GIVEN

        # WHEN
        roles_from_files = LoaderRole.load_roles_from_files()

        # THEN
        expected = 974
        actual = len(roles_from_files)
        self.assertEqual(expected, actual)
