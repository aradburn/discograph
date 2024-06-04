from discograph.library.database.entity_repository import EntityRepository
from discograph.library.loader.loader_entity import LoaderEntity
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderEntityPassTwo(RepositoryTestCase):
    def test_loader_entity_pass_two(self):
        # GIVEN
        date = "test"
        LoaderEntity().loader_pass_one(date)

        # WHEN
        LoaderEntity().loader_pass_two()

        # THEN
        expected = 6216
        actual = EntityRepository().count()
        self.assertEqual(expected, actual)
