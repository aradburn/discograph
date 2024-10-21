from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestLoaderRole(RepositoryTestCase):

    def setUp(self):
        super().setUp()
        self.resetDB()

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
        expected = 991
        actual = len(roles_from_files)
        self.assertEqual(expected, actual)

    def test_load_roles_from_files_from_database(self):
        # GIVEN
        roles_from_files = LoaderRole.load_roles_from_files()

        # WHEN
        LoaderRole.save_roles(roles_from_files)
        LoaderRole.load_all_roles()

        # THEN
        actual = len(roles_from_files)
        expected = len(RoleDataAccess.role_name_to_role_id_lookup)
        self.assertTrue(expected > 0)
        self.assertTrue(expected <= actual)
        self.assertEqual(
            len(RoleDataAccess.role_name_to_role_id_lookup),
            len(RoleDataAccess.role_id_to_role_name_lookup),
        )

    def test_load_hornbostel_sachs_instruments_from_database(self):
        # GIVEN
        hornbostel_sachs_roles = LoaderRole.load_hornbostel_sachs_instruments()

        # WHEN
        LoaderRole.save_roles(hornbostel_sachs_roles)
        LoaderRole.load_all_roles()

        # THEN
        actual = len(hornbostel_sachs_roles)
        expected = len(RoleDataAccess.role_name_to_role_id_lookup)
        self.assertTrue(expected > 0)
        self.assertTrue(expected <= actual)
        self.assertEqual(
            len(RoleDataAccess.role_name_to_role_id_lookup),
            len(RoleDataAccess.role_id_to_role_name_lookup),
        )

    def test_load_wikipedia_instruments_from_database(self):
        # GIVEN
        wikipedia_instruments = LoaderRole.load_wikipedia_instruments()

        # WHEN
        LoaderRole.save_roles(wikipedia_instruments)
        LoaderRole.load_all_roles()

        # THEN
        actual = len(wikipedia_instruments)
        expected = len(RoleDataAccess.role_name_to_role_id_lookup)
        self.assertTrue(expected > 0)
        self.assertTrue(expected <= actual)
        self.assertEqual(
            len(RoleDataAccess.role_name_to_role_id_lookup),
            len(RoleDataAccess.role_id_to_role_name_lookup),
        )
