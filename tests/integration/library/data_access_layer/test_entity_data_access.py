from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.transaction import transaction
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestEntityDataAccess(DatabaseTestCase):

    def test_init_text_search_index(self):
        index = TextSearchIndex()

        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.init_text_search_index(entity_repository, index)

        self.assertEqual(7128, len(index.index.items()))
        self.assertEqual(6216, len(index.documents.items()))

    # def test_text_search_lookup(self):
    #     index = self._db_helper
    #     index.
