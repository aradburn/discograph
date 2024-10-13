__all__ = [
    "Entity",
    # "EntityUncommitted",
]

import logging

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


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
        entity_id, entity_type = self.entity_key
        if entity_type == EntityType.ARTIST:
            return f"artist-{self.entity_id}"
        elif entity_type == EntityType.LABEL:
            return f"label-{self.entity_id}"
        raise ValueError(self.entity_key)

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


# class EntityUncommitted(_EntityBase):
#     """This schema is used for creating an instance without an id before it is persisted into the database."""
#
#     pass


class Entity(_EntityBase):
    id: int
