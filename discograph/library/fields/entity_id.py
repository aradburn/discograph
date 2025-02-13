from discograph.library.fields.entity_type import EntityType

LABEL_ENTITY_ID_OFFSET = 1000000000
MISSING_LABEL_ENTITY = -2000000000


def to_entity_internal_id(entity_id: int, entity_type: EntityType) -> int:
    if entity_type == EntityType.ARTIST:
        assert entity_id != MISSING_LABEL_ENTITY
        assert entity_id < LABEL_ENTITY_ID_OFFSET
        return entity_id
    else:
        if entity_id != MISSING_LABEL_ENTITY:
            if entity_id < LABEL_ENTITY_ID_OFFSET:
                return entity_id + LABEL_ENTITY_ID_OFFSET
            else:
                return entity_id
        else:
            return MISSING_LABEL_ENTITY


def to_entity_external_id(id_: int) -> tuple[int, EntityType]:
    if id_ == MISSING_LABEL_ENTITY:
        entity_id = -1
        entity_type = EntityType.LABEL
    elif id_ >= LABEL_ENTITY_ID_OFFSET:
        entity_id = id_ - LABEL_ENTITY_ID_OFFSET
        entity_type = EntityType.LABEL
    else:
        entity_id = id_
        entity_type = EntityType.ARTIST
    return entity_id, entity_type


def to_entity_label_internal_id(entity_id: int | None) -> int:
    if entity_id:
        return to_entity_internal_id(entity_id, EntityType.LABEL)
    else:
        return MISSING_LABEL_ENTITY
