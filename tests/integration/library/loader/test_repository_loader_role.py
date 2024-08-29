from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderRole(RepositoryTestCase):

    def setUp(self):
        super().setUp()
        self.resetDB()

    def test_load_roles_from_files(self):
        # GIVEN
        actual = LoaderRole.load_roles_from_files()
        LoaderRole.load_all_roles()
        expected = len(RoleDataAccess.role_name_to_role_id_lookup)
        self.assertTrue(expected > 0)
        self.assertEqual(expected, actual)
        self.assertEqual(
            len(RoleDataAccess.role_name_to_role_id_lookup),
            len(RoleDataAccess.role_id_to_role_name_lookup),
        )

    def test_load_hornbostel_sachs_instruments(self):
        # GIVEN
        actual = LoaderRole.load_hornbostel_sachs_instruments()
        LoaderRole.load_all_roles()
        expected = len(RoleDataAccess.role_name_to_role_id_lookup)
        self.assertTrue(expected > 0)
        self.assertEqual(expected, actual)
        self.assertEqual(
            len(RoleDataAccess.role_name_to_role_id_lookup),
            len(RoleDataAccess.role_id_to_role_name_lookup),
        )

    def test_load_wikipedia_instruments(self):
        # GIVEN
        actual = LoaderRole.load_wikipedia_instruments()
        LoaderRole.load_all_roles()
        expected = len(RoleDataAccess.role_name_to_role_id_lookup)
        self.assertTrue(expected > 0)
        self.assertEqual(expected, actual)
        self.assertEqual(
            len(RoleDataAccess.role_name_to_role_id_lookup),
            len(RoleDataAccess.role_id_to_role_name_lookup),
        )
