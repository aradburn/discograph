__all__ = [
    "Entity",
]

import logging
from typing import Self

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class _EntityBase(InternalDomainObject):
    """
    Base class for entities in the domain.

    Attributes:
        entity_id (int): The unique identifier for the entity.
        entity_type (EntityType): The type of the entity (e.g., ARTIST, LABEL).
        entity_name (str): The name of the entity.
        relation_counts (dict | list): The counts of relations associated with the entity.
        entity_metadata (dict | list): Metadata associated with the entity.
        entities (dict | list): Related entities.
        search_content (str): Content used for searching the entity.
    """

    entity_id: int
    entity_type: EntityType
    entity_name: str
    relation_counts: dict | list
    entity_metadata: dict | list
    entities: dict | list
    search_content: str

    @property
    def entity_key(self) -> tuple[int, EntityType]:
        """
        Returns the unique key for the entity.

        Returns:
            tuple[int, EntityType]: A tuple containing the entity ID and entity type.
        """
        return self.entity_id, self.entity_type

    @property
    def json_entity_key(self) -> str:
        """
        Returns the JSON representation of the entity key.

        Returns:
            str: The JSON entity key.
        """
        return self.to_json_entity_key(self.entity_id, self.entity_type)

    @property
    def size(self) -> int:
        """
        Returns the size of the entity based on its type.

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
            entity_id (int): The unique identifier for the entity.
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

    def to_domain(self) -> Self:
        """
        Converts the entity to its domain representation.

        Returns:
            Self: The domain representation of the entity.
        """
        return self

    def to_db(self) -> Self:
        """
        Converts the entity to its database representation.

        Returns:
            Self: The database representation of the entity.
        """
        return self


class Entity(_EntityBase):
    """
    Entity class that extends the base entity class.

    Attributes:
        id (int): The unique identifier for the entity.
    """

    id: int
