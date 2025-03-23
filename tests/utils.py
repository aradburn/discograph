from discograph.config import TEST_DATA_DIR
from discograph.library.fields.entity_type import EntityType
from discograph.offline.domain.entity import Entity
from discograph.offline.domain.release import Release
from discograph.offline.loader.loader_utils import LoaderUtils
from discograph.offline.loader.parser_entity import ParserEntity
from discograph.offline.loader.parser_release import ParserRelease


def get_test_entity_by_id(entity_id: int, entity_type: EntityType) -> Entity:
    iterator = LoaderUtils.get_iterator(
        TEST_DATA_DIR, entity_type.name.lower(), "testinsert"
    )

    while True:
        element = next(iterator)
        entity = ParserEntity().from_element(element)
        if entity.entity_id == entity_id:
            break
    return entity


def get_test_release_by_id(release_id: int) -> Release:
    iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "release", "testinsert")

    while True:
        element = next(iterator)
        release = ParserRelease().from_element(element)
        if release.release_id == release_id:
            break
    return release
