from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderRole(RepositoryTestCase):
    def test_loader_role_pass_one(self):
        date = "test"
        actual = LoaderRole().loader_pass_one(date)
        expected = 953
        self.assertEqual(expected, actual)
