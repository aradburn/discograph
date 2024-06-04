from discograph import utils
from discograph.library.database.role_repository import RoleRepository
from discograph.library.database.transaction import transaction
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestDatabaseRole(DatabaseTestCase):
    def test_from_db_01(self):
        name = "Acoustic Bass"
        with transaction():
            role = RoleRepository().get_by_name(name)
            actual = utils.normalize_dict(role.model_dump())

        expected_role = {
            "role_category": "Category.INSTRUMENTS",
            "role_category_name": "Instruments",
            "role_id": 12,
            "role_name": "Acoustic Bass",
            "role_subcategory": "Subcategory.STRINGED_INSTRUMENTS",
            "role_subcategory_name": "String Instruments",
        }
        expected = utils.normalize_dict(expected_role)
        self.assertEqual(expected, actual)

    def test_from_db_02(self):
        name = "Mezzo-soprano Vocals"
        with transaction():
            role = RoleRepository().get_by_name(name)
            actual = utils.normalize_dict(role.model_dump())

        expected_role = {
            "role_category": "Category.VOCAL",
            "role_category_name": "Vocal",
            "role_id": 544,
            "role_name": "Mezzo-Soprano Vocals",
            "role_subcategory": "Subcategory.NONE",
            "role_subcategory_name": "None",
        }
        expected = utils.normalize_dict(expected_role)
        self.assertEqual(expected, actual)
