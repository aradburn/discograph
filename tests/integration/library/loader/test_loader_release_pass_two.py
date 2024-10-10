from discograph.config import TEST_DATA_DIR
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_release import LoaderRelease
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestLoaderReleasePassTwo(RepositoryTestCase):
    def test_loader_release_pass_two(self):
        # GIVEN
        date = "testinsert"
        LoaderRole().load_all_roles()
        LoaderEntity().loader_entity_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        LoaderRelease().loader_release_pass_one(
            TEST_DATA_DIR, date, is_bulk_inserts=True
        )
        LoaderEntity().loader_entity_pass_two()

        # WHEN
        LoaderRelease().loader_release_pass_two()

        # THEN
        expected = 1700
        actual = ReleaseRepository().count()
        self.assertEqual(expected, actual)
