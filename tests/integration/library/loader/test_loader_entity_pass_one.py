from discograph.config import TEST_DATA_DIR
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestLoaderEntityPassOne(RepositoryTestCase):
    def test_loader_entity_pass_one(self):
        # GIVEN
        date = "testinsert"
        LoaderRole().load_all_roles()

        # WHEN
        actual = LoaderEntity().loader_entity_pass_one(
            TEST_DATA_DIR, date, is_bulk_inserts=True
        )

        # THEN
        expected = 6216
        self.assertEqual(expected, actual)
