from discograph.config import TEST_DATA_DIR
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_relation import LoaderRelation
from discograph.library.loader.loader_release import LoaderRelease
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryLoaderRelationPassTwo(RepositoryTestCase):
    def test_loader_relation_pass_two(self):
        # GIVEN
        date = "testinsert"
        LoaderRole().load_all_roles()
        LoaderEntity().loader_entity_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        LoaderRelease().loader_release_pass_one(
            TEST_DATA_DIR, date, is_bulk_inserts=True
        )
        LoaderEntity().loader_entity_pass_two()
        LoaderRelease().loader_release_pass_two()
        LoaderRelation().loader_relation_pass_one(date)

        # WHEN
        LoaderRelation().loader_relation_pass_two(date)

        # THEN
        expected = 15814
        actual = RelationRepository().count()
        self.assertEqual(expected, actual)
