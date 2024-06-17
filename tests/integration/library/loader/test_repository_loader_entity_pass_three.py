from discograph.config import TEST_DATA_DIR
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_relation import LoaderRelation
from discograph.library.loader.loader_release import LoaderRelease
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderEntityPassThree(RepositoryTestCase):
    def test_loader_entity_pass_three(self):
        # GIVEN
        date = "testinsert"
        LoaderRole().loader_pass_one(date)
        LoaderEntity().loader_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        LoaderRelease().loader_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        LoaderEntity().loader_pass_two()
        LoaderRelease().loader_pass_two()
        LoaderRelation().loader_relation_pass_one(date)
        # RelationRepository().delete_relation_duplicates()
        # LoaderRelation().loader_relation_pass_two(date)

        # WHEN
        LoaderEntity().loader_pass_three()

        # THEN
        expected = 6216
        actual = EntityRepository().count()
        self.assertEqual(expected, actual)
