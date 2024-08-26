import unittest

from discograph.library.loader.loader_role import LoaderRole


class TestLoaderRole(unittest.TestCase):
    def test_load_instruments(self):
        # GIVEN
        num_instruments = LoaderRole().load_instruments()
        expected = 643
        self.assertEqual(expected, num_instruments)
