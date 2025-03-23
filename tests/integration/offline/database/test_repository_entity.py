from discograph.config import TEST_DATA_DIR
from discograph.library.fields.entity_type import EntityType
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.database.offline_transaction import offline_transaction
from discograph.offline.loader.loader_utils import LoaderUtils
from discograph.offline.loader.parser_entity import ParserEntity
from tests.integration.offline.database.offline_repository_test_case import (
    OfflineRepositoryTestCase,
)


class TestRepositoryEntity(OfflineRepositoryTestCase):
    def test_create_01(self):
        # GIVEN
        iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "artist", "testinsert")
        entity_element = next(iterator)
        entity = ParserEntity().from_element(entity_element)

        # WHEN
        with offline_transaction():
            repository = EntityRepository()
            created_entity = repository.create(entity)

        # THEN
        self.assertEqual(entity, created_entity)

    def test_get_01(self):
        # GIVEN
        iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "label", "testinsert")
        entity_element = next(iterator)
        entity = ParserEntity().from_element(entity_element)

        # WHEN
        with offline_transaction():
            repository = EntityRepository()
            created_entity = repository.create(entity)

            retrieved_entity = repository.get_by_entity_id_and_entity_type(
                1, EntityType.LABEL
            )

        # THEN
        self.assertEqual(created_entity, retrieved_entity)
