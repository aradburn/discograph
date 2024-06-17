from discograph.config import TEST_DATA_DIR
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_relation import LoaderRelation
from discograph.library.loader.loader_release import LoaderRelease
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderRelationPassOne(RepositoryTestCase):
    def test_loader_relation_pass_one(self):
        # GIVEN
        date = "testinsert"
        LoaderRole().loader_pass_one(date)
        LoaderEntity().loader_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        LoaderRelease().loader_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        LoaderEntity().loader_pass_two()
        LoaderRelease().loader_pass_two()

        # WHEN
        LoaderRelation().loader_relation_pass_one(date)
        # RelationRepository().delete_relation_duplicates()

        # THEN
        expected = 16467
        actual = RelationRepository().count()
        self.assertEqual(expected, actual)
