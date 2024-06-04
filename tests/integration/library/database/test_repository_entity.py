from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.transaction import transaction
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader_utils import LoaderUtils
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryEntity(RepositoryTestCase):
    def test_create_01(self):
        # GIVEN
        iterator = LoaderUtils.get_iterator("artist", "test")
        entity_element = next(iterator)
        entity = LoaderEntity().from_element(entity_element)

        # WHEN
        with transaction():
            repository = EntityRepository()
            created_entity = repository.create(entity)

        # THEN
        self.assertEqual(entity, created_entity)

    def test_get_01(self):
        # GIVEN
        iterator = LoaderUtils.get_iterator("label", "test")
        entity_element = next(iterator)
        entity = LoaderEntity().from_element(entity_element)

        # WHEN
        with transaction():
            repository = EntityRepository()
            created_entity = repository.create(entity)

            retrieved_entity = repository.get(1, EntityType.LABEL)

        # THEN
        self.assertEqual(created_entity, retrieved_entity)
