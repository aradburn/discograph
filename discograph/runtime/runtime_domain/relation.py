import logging
from typing import Dict, Any

from discograph import utils
from discograph.library.cache.role_cache import RoleCache
from discograph.library.domain.base import InternalDomainObject

__all__ = [
    "RuntimeRelationUncommitted",
    "RuntimeRelationDB",
    "RuntimeRelation",
    "RuntimeRelationInternal",
    "RuntimeRelationResult",
]

from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class _RuntimeRelationBase(InternalDomainObject):
    """
    Base class for runtime relation entities.
    """

    pass


class RuntimeRelationUncommitted(_RuntimeRelationBase):
    """
    This schema is used for creating an instance without an id before it is persisted into the database.

    Attributes:
        subject (int): The ID of the subject entity.
        role_name (str): The name of the role.
        object (int): The ID of the object entity.
    """

    subject: int
    role_name: str
    object: int


class RuntimeRelationDB(_RuntimeRelationBase):
    """
    Saved Relation representation, database internal representation.

    Attributes:
        id (int): The unique identifier for the relation.
        subject (int): The ID of the subject entity.
        predicate (int): The ID of the predicate entity.
        object (int): The ID of the object entity.
    """

    id: int
    subject: int
    predicate: int
    object: int

    def to_domain(self) -> "RuntimeRelationInternal":
        """
        Converts the RuntimeRelationDB instance to a RuntimeRelationInternal instance.

        Returns:
            RuntimeRelationInternal: The internal representation of the relation.
        """
        relation_db_dict: dict = self.model_dump()
        role_id: int = relation_db_dict.get("predicate")
        role_name = RoleCache.role_id_to_role_name_lookup[role_id]
        relation_db_dict.update(role=role_name)
        return RuntimeRelationInternal.model_validate(relation_db_dict)


class RuntimeRelationInternal(_RuntimeRelationBase):
    """
    Saved Relation representation, database internal representation.

    Attributes:
        id (int): The unique identifier for the relation.
        subject (int): The ID of the subject entity.
        role (str): The role of the relation.
        object (int): The ID of the object entity.
    """

    id: int
    subject: int
    role: str
    object: int


class RuntimeRelation(_RuntimeRelationBase):
    """
    Domain Relation representation, public facing.

    Attributes:
        id (int): The unique identifier for the relation.
        entity_one_id (int): The ID of the first entity.
        entity_one_type (EntityType): The type of the first entity.
        entity_two_id (int): The ID of the second entity.
        entity_two_type (EntityType): The type of the second entity.
        role (str): The role of the relation.
        releases (Dict[str, int | None] | None): The releases associated with the relation.
    """

    id: int
    entity_one_id: int
    entity_one_type: EntityType
    entity_two_id: int
    entity_two_type: EntityType
    role: str
    releases: Dict[str, int | None] | None = None

    @property
    def entity_one_key(self) -> tuple[int, EntityType]:
        """
        Returns the key for the first entity.

        Returns:
            tuple[int, EntityType]: The key for the first entity.
        """
        return self.entity_one_id, self.entity_one_type

    @property
    def entity_two_key(self) -> tuple[int, EntityType]:
        """
        Returns the key for the second entity.

        Returns:
            tuple[int, EntityType]: The key for the second entity.
        """
        return self.entity_two_id, self.entity_two_type

    @property
    def json_entity_one_key(self) -> str:
        """
        Returns the JSON representation of the first entity key.

        Returns:
            str: The JSON entity key for the first entity.

        Raises:
            ValueError: If the entity type is not recognized.
        """
        if self.entity_one_type == EntityType.ARTIST:
            return f"artist-{self.entity_one_id}"
        elif self.entity_one_type == EntityType.LABEL:
            return f"label-{self.entity_one_id}"
        raise ValueError(self.entity_one_key)

    @property
    def json_entity_two_key(self) -> str:
        """
        Returns the JSON representation of the second entity key.

        Returns:
            str: The JSON entity key for the second entity.

        Raises:
            ValueError: If the entity type is not recognized.
        """
        if self.entity_two_type == EntityType.ARTIST:
            return f"artist-{self.entity_two_id}"
        elif self.entity_two_type == EntityType.LABEL:
            return f"label-{self.entity_two_id}"
        raise ValueError(self.entity_two_key)

    @property
    def link_key(self) -> str:
        """
        Returns the link key for the relation.

        Returns:
            str: The link key for the relation.
        """
        source = self.json_entity_one_key
        target = self.json_entity_two_key
        role = utils.WORD_PATTERN.sub("-", str(self.role)).lower()
        pieces = [
            source,
            role,
            target,
        ]
        return "-".join(str(_) for _ in pieces)


class RuntimeRelationResult(RuntimeRelation):
    """
    Domain Search result Relation representation, public facing.

    Attributes:
        id (int): The unique identifier for the relation.
        role (str): The role of the relation.
        distance (int | None): The distance of the relation, if available.
    """

    id: int
    role: str
    distance: int | None = None

    def as_json(self) -> Dict[str, Any]:
        """
        Converts the relation result to a JSON representation.

        Returns:
            Dict[str, Any]: The JSON representation of the relation result.
        """
        data = {
            "key": self.link_key,
            "role": self.role,
            "source": self.json_entity_one_key,
            "target": self.json_entity_two_key,
        }
        if hasattr(self, "distance"):
            data["distance"] = self.distance
        return data
