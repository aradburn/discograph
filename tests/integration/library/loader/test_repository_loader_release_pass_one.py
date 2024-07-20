from discograph.config import TEST_DATA_DIR
from discograph.library.loader.loader_release import LoaderRelease
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderReleasePassOne(RepositoryTestCase):
    def test_loader_release_pass_one(self):
        # GIVEN
        date = "testinsert"
        LoaderRole().load_all_roles()

        # WHEN
        actual = LoaderRelease().loader_pass_one(
            TEST_DATA_DIR, date, is_bulk_inserts=True
        )

        # THEN
        expected = 1700
        self.assertEqual(expected, actual)
