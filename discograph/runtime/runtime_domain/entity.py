__all__ = [
    "RuntimeEntity",
    "RuntimeEntityDB",
]

import logging

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class RuntimeEntity(InternalDomainObject):
    """
    Represents a runtime entity.

    Attributes:
        id (int): The unique identifier for the runtime entity.
        entity_id (int): The ID of the entity.
        entity_type (EntityType): The type of the entity.
        entity_name (str): The name of the entity.
        relation_counts (dict | list): The relation counts of the entity.
        entity_metadata (dict | list): The metadata of the entity.
        entities (dict | list): The entities related to this entity.
        countries (str | None): The countries associated with the entity.
        genres (str | None): The genres associated with the entity.
        styles (str | None): The styles associated with the entity.
    """

    id: int
    entity_id: int
    entity_type: EntityType
    entity_name: str
    relation_counts: dict | list
    entity_metadata: dict | list
    entities: dict | list
    countries: str | None = None
    genres: str | None = None
    styles: str | None = None

    @property
    def entity_key(self) -> tuple[int, EntityType]:
        """
        Returns the key for the entity.

        Returns:
            tuple[int, EntityType]: The key for the entity.
        """
        return self.entity_id, self.entity_type

    @property
    def json_entity_key(self) -> str:
        """
        Returns the JSON representation of the entity key.

        Returns:
            str: The JSON entity key for the entity.
        """
        return self.to_json_entity_key(self.entity_id, self.entity_type)

    @property
    def size(self) -> int:
        """
        Returns the size of the entity.

        Returns:
            int: The size of the entity.
        """
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
        """
        Converts the entity ID and type to a JSON entity key.

        Args:
            entity_id (int): The ID of the entity.
            entity_type (EntityType): The type of the entity.

        Returns:
            str: The JSON entity key.

        Raises:
            ValueError: If the entity type is not recognized.
        """
        if entity_type == EntityType.ARTIST:
            return f"artist-{entity_id}"
        elif entity_type == EntityType.LABEL:
            return f"label-{entity_id}"
        raise ValueError(entity_id, entity_type)

    def to_db(self) -> "RuntimeEntityDB":
        """
        Converts the runtime entity to its database representation.

        Returns:
            RuntimeEntityDB: The database representation of the runtime entity.
        """
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
    """
    Represents a runtime entity in the database.

    Attributes:
        id (int): The unique identifier for the runtime entity.
        entity_id (int): The ID of the entity.
        entity_type (EntityType): The type of the entity.
        entity_name (str): The name of the entity.
        relation_counts (dict | list): The relation counts of the entity.
        entity_metadata (dict | list): The metadata of the entity.
        aliases (dict | list | None): The aliases of the entity.
        groups (dict | list | None): The groups associated with the entity.
        members (dict | list | None): The members associated with the entity.
        countries (str | None): The countries associated with the entity.
        genres (str | None): The genres associated with the entity.
        styles (str | None): The styles associated with the entity.
    """

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
        """
        Converts the runtime entity from its database representation to its domain representation.

        Returns:
            RuntimeEntity: The domain representation of the runtime entity.
        """
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
