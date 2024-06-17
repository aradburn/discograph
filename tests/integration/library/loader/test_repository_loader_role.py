from discograph.library.fields.role_type import RoleType
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderRole(RepositoryTestCase):
    def test_loader_role_pass_one(self):
        date = "testinsert"
        actual = LoaderRole().loader_pass_one(date)
        expected = len(RoleType.role_name_to_role_id_lookup)
        self.assertTrue(expected > 0)
        self.assertEqual(expected, actual)
        self.assertEqual(
            len(RoleType.role_name_to_role_id_lookup),
            len(RoleType.role_id_to_role_name_lookup),
        )
