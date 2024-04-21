from discograph.library.database.role_repository import RoleRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.role import RoleUncommited, Role
from discograph.library.fields.role_type import RoleType
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryRole(RepositoryTestCase):
    def test_create_01(self):
        # GIVEN
        role = RoleUncommited(
            role_name="role_name_1",
            role_category=RoleType.Category.INSTRUMENTS,
            role_subcategory=RoleType.Subcategory.KEYBOARD,
            role_category_name="category_name_1",
            role_subcategory_name="subcategory_name_1",
        )
        expected_role = Role(
            role_id=1,
            role_name="role_name_1",
            role_category=RoleType.Category.INSTRUMENTS,
            role_subcategory=RoleType.Subcategory.KEYBOARD,
            role_category_name="category_name_1",
            role_subcategory_name="subcategory_name_1",
        )

        # WHEN
        with transaction():
            repository = RoleRepository()
            created_role = repository.create(role)

        # THEN
        self.assertEqual(expected_role, created_role)

    def test_get_01(self):
        # GIVEN
        role = RoleUncommited(
            role_name="role_name_2",
            role_category=RoleType.Category.REMIX,
            role_subcategory=RoleType.Subcategory.NONE,
            role_category_name="category_name_2",
            role_subcategory_name="subcategory_name_2",
        )

        # WHEN
        with transaction():
            repository = RoleRepository()
            created_role = repository.create(role)

            retrieved_role = repository.get(2)

        # THEN
        self.assertEqual(created_role, retrieved_role)
