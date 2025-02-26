__all__ = [
    "RuntimeEntity",
    "RuntimeEntityDB",
]

import logging

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class RuntimeEntity(InternalDomainObject):
    id: int
    entity_id: int
    entity_type: EntityType
    entity_name: str
    relation_counts: dict | list
    entity_metadata: dict | list
    entities: dict | list
    # search_content: str
    countries: str | None = None
    genres: str | None = None
    styles: str | None = None

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

    def to_db(self) -> "RuntimeEntityDB":
        entity_dict: dict = self.model_dump()
        entities: dict = entity_dict.pop("entities")
        aliases: dict | None = entities.get("aliases")
        groups: dict | None = entities.get("groups")
        members: dict | None = entities.get("members")
        if aliases is not None and len(aliases) == 0:
            aliases = None
        if groups is not None and len(groups) == 0:
            groups = None
        if members is not None and len(members) == 0:
            members = None
        entity_dict.update(aliases=aliases, groups=groups, members=members)
        return RuntimeEntityDB.model_validate(entity_dict)


class RuntimeEntityDB(InternalDomainObject):
    id: int
    entity_id: int
    entity_type: EntityType
    entity_name: str
    relation_counts: dict | list
    entity_metadata: dict | list
    aliases: dict | list | None = None
    groups: dict | list | None = None
    members: dict | list | None = None
    countries: str | None = None
    genres: str | None = None
    styles: str | None = None

    def to_domain(self) -> RuntimeEntity:
        entity_dict: dict = self.model_dump()
        aliases: dict = entity_dict.pop("aliases")
        groups: dict = entity_dict.pop("groups")
        members: dict = entity_dict.pop("members")
        entities = {}
        if aliases is not None:
            entities.update(aliases=aliases)
        if groups is not None:
            entities.update(groups=groups)
        if members is not None:
            entities.update(members=members)
        entity_dict.update(entities=entities)
        return RuntimeEntity.model_validate(entity_dict)
