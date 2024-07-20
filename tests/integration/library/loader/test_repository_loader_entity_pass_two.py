from discograph.config import TEST_DATA_DIR
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderEntityPassTwo(RepositoryTestCase):
    def test_loader_entity_pass_two(self):
        # GIVEN
        date = "testinsert"
        LoaderRole().load_all_roles()
        LoaderEntity().loader_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)

        # WHEN
        LoaderEntity().loader_pass_two()

        # THEN
        expected = 6216
        actual = EntityRepository().count()
        self.assertEqual(expected, actual)
