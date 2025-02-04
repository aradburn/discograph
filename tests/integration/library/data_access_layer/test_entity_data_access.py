from discograph.config import TEST_TEXT_SEARCH_PATH
from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.transaction import transaction
from discograph.library.fields.entity_type import EntityType
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.library.loader.loader_entity import LoaderEntity
from tests import utils
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestEntityDataAccess(DatabaseTestCase):

    def test_init_text_search_index(self):
        index = TextSearchIndex()

        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.init_text_search_index(entity_repository, index)

        # THEN
        self.assertEqual(7256, len(index.index.items()))
        self.assertEqual(6216, len(index.documents.items()))

    def test_text_search_lookup_1(self):
        self._db_helper.text_search_index = LoaderEntity.loader_init_text_search_index(
            TEST_TEXT_SEARCH_PATH
        )
        results = EntityDataAccess.search_entities("Wax")

        # THEN
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
        self._db_helper.text_search_index = LoaderEntity.loader_init_text_search_index(
            TEST_TEXT_SEARCH_PATH
        )
        results = EntityDataAccess.search_entities("Joker")

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
            entity_repository = EntityRepository()
            result = EntityDataAccess.get_id_by_entity_type_and_entity_name(
                entity_repository, entity_type, entity_name
            )

        # THEN
        expected = 8526
        self.assertEqual(expected, result)

    def test_resolve_entity_references_1(self):
        # GIVEN
        entity_id = 48
        entity_type = EntityType.ARTIST
        entity = utils.get_test_entity_by_id(entity_id, entity_type)

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_entity_references(entity_repository, entity)
            result = entity.entities

        # THEN
        expected = {
            "aliases": {
                "Aphex Twin": 45,
                "Dice Man, The": 820,
                "Polygon Window": 2931,
                "Richard D. James": 435132,
            }
        }
        self.assertEqual(expected, result)

    def test_resolve_entity_references_2(self):
        # GIVEN
        entity_id = 98
        entity_type = EntityType.ARTIST
        entity = utils.get_test_entity_by_id(entity_id, entity_type)

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_entity_references(entity_repository, entity)
            result = entity.entities

        # THEN
        expected = {
            "aliases": {"Cosmos": 14168},
            "groups": {
                "Chameleon": 1798,
                "Global Communication": 79,
                "Jedi Knights": 1799,
                "Link & E621": 5131,
                "Reload": 1791,
            },
        }
        self.assertEqual(expected, result)

    def test_resolve_entity_references_3(self):
        entity_id = 288
        entity_type = EntityType.ARTIST
        entity = utils.get_test_entity_by_id(entity_id, entity_type)

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_entity_references(entity_repository, entity)
            result = entity.entities

        # THEN
        expected = {
            "aliases": {},
            "members": {
                "Alex Banks": 10141,
                "Jay Hurren": 474638,
            },
        }
        self.assertEqual(expected, result)

    def test_resolve_entity_references_4(self):
        # GIVEN
        entity_id = 61
        entity_type = EntityType.LABEL
        entity = utils.get_test_entity_by_id(entity_id, entity_type)

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_entity_references(entity_repository, entity)
            result = entity.entities

        # THEN
        expected = {
            "parent_label": {
                "Instinct Records": 1000000063,
            }
        }
        self.assertEqual(expected, result)

    def test_resolve_release_references_1(self):
        # GIVEN
        release_id = 637
        release = utils.get_test_release_by_id(release_id)

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_release_references(entity_repository, release)
            result = release.labels

        # THEN
        expected = [
            {"catalog_number": "WAP100CD", "id": 1000023528, "name": "Warp Records"},
            {"catalog_number": "WAP 100CD", "id": 1000023528, "name": "Warp Records"},
        ]
        self.assertEqual(expected, result)

    def test_resolve_release_references_2(self):
        # GIVEN
        release_id = 158
        release = utils.get_test_release_by_id(release_id)

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_release_references(entity_repository, release)
            result = release.companies

        # THEN
        expected = [
            {
                "entity_type": 13,
                "entity_type_name": "Phonographic Copyright (p)",
                "id": 1000264514,
                "name": "Warp Records Limited",
            },
            {
                "entity_type": 14,
                "entity_type_name": "Copyright (c)",
                "id": 1000264514,
                "name": "Warp Records Limited",
            },
            {
                "entity_type": 21,
                "entity_type_name": "Published By",
                "id": 1000265170,
                "name": "Warp Music",
            },
            {
                "entity_type": 21,
                "entity_type_name": "Published By",
                "id": 1000045746,
                "name": "EMI Music",
            },
            {
                "entity_type": 17,
                "entity_type_name": "Pressed By",
                "id": 1000147881,
                "name": "Mayking",
            },
        ]
        self.assertEqual(expected, result)

    def test_resolve_release_references_3(self):
        # GIVEN
        release_id = 1700
        release = utils.get_test_release_by_id(release_id)
        print(f"release before: {release}")

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_release_references(entity_repository, release)
            print(f"release after : {release}")
            result = release.artists

        # THEN
        expected = [{"id": 0, "name": "Various"}]
        self.assertEqual(expected, result)

    def test_resolve_release_references_4(self):
        # GIVEN
        release_id = 1700
        release = utils.get_test_release_by_id(release_id)
        print(f"release before: {release}")

        # WHEN
        with transaction():
            entity_repository = EntityRepository()
            EntityDataAccess.resolve_release_references(entity_repository, release)
            print(f"release after : {release}")
            result = release.extra_artists

        # THEN
        expected = [
            {
                "id": 1548777,
                "name": "Phil Wolstenholme",
                "roles": [{"detail": "Digital Holme-grown", "name": "Artwork"}],
            },
            {
                "id": 445854,
                "name": "Designers Republic, The",
                "roles": [{"detail": "Piezoelectric Warriors", "name": "Artwork"}],
            },
            {"id": 391, "name": "David Toop", "roles": [{"name": "Liner Notes"}]},
        ]
        self.assertEqual(expected, result)
