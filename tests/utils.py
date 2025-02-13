from discograph.config import TEST_DATA_DIR
from discograph.offline.domain.entity import Entity
from discograph.offline.domain.release import Release
from discograph.library.fields.entity_type import EntityType
from discograph.offline.loader.loader_entity import LoaderEntity
from discograph.offline.loader.loader_release import LoaderRelease
from discograph.offline.loader.loader_utils import LoaderUtils


def get_test_entity_by_id(entity_id: int, entity_type: EntityType) -> Entity:
    iterator = LoaderUtils.get_iterator(
        TEST_DATA_DIR, entity_type.name.lower(), "testinsert"
    )

    while True:
        element = next(iterator)
        entity = LoaderEntity().from_element(element)
        if entity.entity_id == entity_id:
            break
    return entity


def get_test_release_by_id(release_id: int) -> Release:
    iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "release", "testinsert")

    while True:
        element = next(iterator)
        release = LoaderRelease().from_element(element)
        if release.release_id == release_id:
            break
    return release
