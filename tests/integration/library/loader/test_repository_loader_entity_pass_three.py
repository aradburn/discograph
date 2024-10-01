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
        LoaderRole().load_all_roles()
        LoaderEntity().loader_entity_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        LoaderRelease().loader_release_pass_one(
            TEST_DATA_DIR, date, is_bulk_inserts=True
        )
        LoaderEntity().loader_entity_pass_two()
        LoaderRelease().loader_release_pass_two()
        LoaderRelation().loader_relation_pass_one(date)
        LoaderRelation().loader_relation_pass_two(date)

        # WHEN
        LoaderEntity().loader_entity_pass_three()

        # THEN
        expected = 6216
        actual = EntityRepository().count()
        self.assertEqual(expected, actual)
