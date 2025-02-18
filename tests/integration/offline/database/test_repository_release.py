from discograph.config import TEST_DATA_DIR
from discograph.offline.database.release_repository import ReleaseRepository
from discograph.offline.database.offline_transaction import offline_transaction
from discograph.offline.loader.loader_release import LoaderRelease
from discograph.offline.loader.loader_utils import LoaderUtils
from tests.integration.offline.database.offline_repository_test_case import (
    OfflineRepositoryTestCase,
)


class TestRepositoryRelease(OfflineRepositoryTestCase):
    def test_create_01(self):
        # GIVEN
        iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "release", "testinsert")
        release_element = next(iterator)
        release = LoaderRelease().from_element(release_element)

        # WHEN
        with offline_transaction():
            repository = ReleaseRepository()
            created_release = repository.create(release)

        # THEN
        self.assertEqual(release, created_release)

    def test_get_01(self):
        # GIVEN
        iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "release", "testinsert")
        next(iterator)
        next(iterator)
        next(iterator)
        next(iterator)
        next(iterator)
        next(iterator)
        release_element = next(iterator)
        release = LoaderRelease().from_element(release_element)

        # WHEN
        with offline_transaction():
            repository = ReleaseRepository()
            created_release = repository.create(release)

            retrieved_release = repository.get(635)

        # THEN
        self.assertEqual(created_release, retrieved_release)
