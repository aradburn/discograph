from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestLoaderRelationPassTwo(RepositoryTestCase):
    def test_loader_relation_pass_two(self):
        pass
        # GIVEN
        # date = "testinsert"
        # LoaderRole().load_all_roles()
        # LoaderEntity().loader_entity_pass_one(TEST_DATA_DIR, date, is_bulk_inserts=True)
        # LoaderRelease().loader_release_pass_one(
        #     TEST_DATA_DIR, date, is_bulk_inserts=True
        # )
        # LoaderEntity().loader_entity_pass_two()
        # LoaderRelease().loader_release_pass_two()
        # LoaderRelation().loader_relation_pass_one(date)
        #
        # # WHEN
        # LoaderRelation().loader_relation_pass_two(date)
        #
        # # THEN
        # expected = 15814
        # actual = RelationRepository().count()
        # self.assertEqual(expected, actual)
