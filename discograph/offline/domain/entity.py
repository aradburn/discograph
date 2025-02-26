__all__ = [
    "Entity",
]

import logging
from typing import Self

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

    def to_domain(self) -> Self:
        # Domain and Database entities are the same
        return self

    def to_db(self) -> Self:
        # Domain and Database entities are the same
        return self


class Entity(_EntityBase):
    id: int
