from discograph import utils
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestDatabaseRole(DatabaseTestCase):
    def test_from_db_01(self):
        name = "Acoustic Bass"
        with self.test_session.begin() as session:
            role = DatabaseTestCase.role.get_by_name(session, name)
            actual = utils.normalize(format(role))

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
        with self.test_session.begin() as session:
            role = DatabaseTestCase.role.get_by_name(session, name)
            actual = utils.normalize(format(role))

        expected_role = {
            "role_category": "Category.VOCAL",
            "role_category_name": "Vocal",
            "role_id": 543,
            "role_name": "Mezzo-Soprano Vocals",
            "role_subcategory": "Subcategory.NONE",
            "role_subcategory_name": "None",
        }
        expected = utils.normalize_dict(expected_role)
        self.assertEqual(expected, actual)
