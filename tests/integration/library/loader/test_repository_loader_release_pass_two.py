from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_release import LoaderRelease
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderReleasePassTwo(RepositoryTestCase):
    def test_loader_release_pass_two(self):
        # GIVEN
        date = "test"
        LoaderEntity().loader_pass_one(date)
        LoaderRelease().loader_pass_one(date)
        LoaderEntity().loader_pass_two()

        # WHEN
        LoaderRelease().loader_pass_two()

        # THEN
        expected = 1700
        actual = ReleaseRepository().count()
        self.assertEqual(expected, actual)
