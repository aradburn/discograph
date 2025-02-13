from discograph.config import TEXT_SEARCH_PATH
from discograph.library.fields.entity_type import EntityType
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.runtime.data_access_layer.runtime_entity_data_access import (
    RuntimeEntityDataAccess,
)
from discograph.runtime.runtime_database.runtime_entity_repository import (
    RuntimeEntityRepository,
)
from discograph.runtime.runtime_database.runtime_transaction import transaction
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
from tests.integration.runtime.database.runtime_database_test_case import (
    RuntimeDatabaseTestCase,
)


class TestRuntimeEntityDataAccess(RuntimeDatabaseTestCase):

    def test_text_search_lookup_1(self):
        RuntimeDatabaseManager.runtime_db_helper.text_search_index = (
            TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
        )
        results = RuntimeEntityDataAccess.search_entities("Wax")

        # THEN
        expected = [
            {"key": "artist-333377", "name": "Wax (10)"},
            {"key": "artist-1163252", "name": "Wax (19)"},
            {"key": "artist-288583", "name": "Wax Tailor"},
            {"key": "artist-785", "name": "Wax Doctor"},
            {"key": "artist-46488", "name": "Wax Poetic"},
            {"key": "artist-242216", "name": "Lord Wax"},
            {"key": "artist-25723", "name": "Freshmess On Wax"},
            {"key": "artist-759", "name": "Nightmares On Wax"},
            {"key": "label-10693", "name": "Wax Magazine"},
            {"key": "label-173661", "name": "Wax Treatment"},
            {"key": "label-953", "name": "Wax Trax! Records"},
            {"key": "label-294161", "name": "Wax Trax! Records, Inc."},
            {"key": "label-111", "name": "Mo Wax"},
            {"key": "label-290481", "name": "Mo Wax Recordings"},
        ]
        self.assertEqual(14, len(results["results"]))
        self.assertEqual(expected, list(results["results"]))

    def test_text_search_lookup_2(self):
        RuntimeDatabaseManager.runtime_db_helper.text_search_index = (
            TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
        )
        results = RuntimeEntityDataAccess.search_entities("Joker")

        # THEN
        expected = [
            {"key": "artist-622822", "name": "Joker (5)"},
            {"key": "artist-8526", "name": "Joker, The (3)"},
            {"key": "artist-129882", "name": "Joker, The (4)"},
        ]
        self.assertEqual(3, len(results["results"]))
        self.assertEqual(expected, list(results["results"]))

    def test_get_id_by_entity_type_and_entity_name(self):
        entity_type = EntityType.ARTIST
        entity_name = "Joker, The (3)"
        with transaction():
            entity_repository = RuntimeEntityRepository()
            result = RuntimeEntityDataAccess.get_id_by_entity_type_and_entity_name(
                entity_repository, entity_type, entity_name
            )

        # THEN
        expected = 8526
        self.assertEqual(expected, result)
