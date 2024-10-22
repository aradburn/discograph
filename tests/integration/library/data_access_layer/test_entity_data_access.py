from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.transaction import transaction
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.library.loader.loader_entity import LoaderEntity
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestEntityDataAccess(DatabaseTestCase):

    def test_init_text_search_index(self):
        index = TextSearchIndex()

        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.init_text_search_index(entity_repository, index)

        self.assertEqual(7253, len(index.index.items()))
        self.assertEqual(6216, len(index.documents.items()))

    def test_text_search_lookup_1(self):
        self._db_helper.text_search_index = LoaderEntity.loader_init_text_search_index()
        results = EntityDataAccess.search_entities("Wax")
        expected = [
            {"key": "artist-333377", "name": "Wax (10)"},
            {"key": "artist-1163252", "name": "Wax (19)"},
            {"key": "artist-288583", "name": "Wax Tailor"},
            {"key": "artist-785", "name": "Wax Doctor"},
            {"key": "artist-46488", "name": "Wax Poetic"},
            {"key": "label-10693", "name": "Wax Magazine"},
            {"key": "label-173661", "name": "Wax Treatment"},
            {"key": "label-953", "name": "Wax Trax! Records"},
            {"key": "label-294161", "name": "Wax Trax! Records, Inc."},
            {"key": "label-111", "name": "Mo Wax"},
            {"key": "artist-242216", "name": "Lord Wax"},
            {"key": "artist-25723", "name": "Freshmess On Wax"},
            {"key": "artist-759", "name": "Nightmares On Wax"},
            {"key": "label-290481", "name": "Mo Wax Recordings"},
        ]
        self.assertEqual(14, len(results["results"]))
        self.assertEqual(expected, list(results["results"]))

    def test_text_search_lookup_2(self):
        self._db_helper.text_search_index = LoaderEntity.loader_init_text_search_index()
        results = EntityDataAccess.search_entities("Joker")
        expected = [
            {"key": "artist-622822", "name": "Joker (5)"},
            {"key": "artist-8526", "name": "Joker, The (3)"},
            {"key": "artist-129882", "name": "Joker, The (4)"},
        ]
        self.assertEqual(3, len(results["results"]))
        self.assertEqual(expected, list(results["results"]))
