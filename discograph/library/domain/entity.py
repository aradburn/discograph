__all__ = [
    "Entity",
    # "EntityUncommitted",
]

import logging

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)

LABEL_ENTITY_ID_OFFSET = 1000000000
MISSING_LABEL_ENTITY = -2000000000


class _EntityBase(InternalDomainObject):
    entity_id: int
    entity_type: EntityType
    entity_name: str
    relation_counts: dict | list
    entity_metadata: dict | list
    entities: dict | list
    search_content: str
    random: float

    # PUBLIC PROPERTIES

    @property
    def entity_key(self) -> tuple[int, EntityType]:
        return self.entity_id, self.entity_type

    @property
    def json_entity_key(self) -> str:
        return self.to_json_entity_key(self.entity_id, self.entity_type)

    @property
    def size(self) -> int:
        members = []
        if self.entity_type == EntityType.ARTIST:
            if "members" in self.entities:
                members = self.entities["members"]
        elif self.entity_type == EntityType.LABEL:
            if "sublabels" in self.entities:
                members = self.entities["sublabels"]
        return len(members)

    @staticmethod
    def to_json_entity_key(entity_id: int, entity_type: EntityType) -> str:
        if entity_type == EntityType.ARTIST:
            return f"artist-{entity_id}"
        elif entity_type == EntityType.LABEL:
            return f"label-{entity_id}"
        raise ValueError(entity_id, entity_type)

    @staticmethod
    def to_entity_internal_id(entity_id: int, entity_type: EntityType) -> int:
        if entity_type == EntityType.ARTIST:
            return entity_id
        else:
            if entity_id != MISSING_LABEL_ENTITY:
                return entity_id + LABEL_ENTITY_ID_OFFSET
            else:
                return MISSING_LABEL_ENTITY

    @staticmethod
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

    @staticmethod
    def to_entity_label_id(entity_id: int | None) -> int:
        if entity_id:
            return entity_id
        else:
            return MISSING_LABEL_ENTITY


# class EntityUncommitted(_EntityBase):
#     """This schema is used for creating an instance without an id before it is persisted into the database."""
#
#     pass


class Entity(_EntityBase):
    id: int
