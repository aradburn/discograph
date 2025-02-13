from discograph.offline.database.release_repository import ReleaseRepository
from tests.integration.offline.database.repository_test_case import RepositoryTestCase


class TestLoaderReleasePassOne(RepositoryTestCase):
    def test_loader_release_pass_one(self):
        # GIVEN

        # WHEN
        actual = ReleaseRepository().count()

        # THEN
        expected = 1700
        self.assertEqual(expected, actual)
