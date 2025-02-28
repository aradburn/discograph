import logging
from typing import Dict, Any, Self

from discograph import utils
from discograph.exceptions import NotFoundError
from discograph.library.cache.role_cache import RoleCache
from discograph.library.domain.base import InternalDomainObject

__all__ = [
    "RelationUncommitted",
    "RelationDB",
    "Relation",
    "RelationInternal",
    "RelationResult",
]

from discograph.library.fields.entity_id import to_entity_external_id

from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class _RelationBase(InternalDomainObject):
    """Base class for relation entities."""

    pass


class RelationUncommitted(_RelationBase):
    """
    This schema is used for creating an instance without an id before it is persisted into the database.

    Attributes:
        subject (int): The subject entity ID.
        role_name (str): The name of the role.
        object (int): The object entity ID.
    """

    subject: int
    role_name: str
    object: int


class RelationDB(_RelationBase):
    """
    Saved Relation representation, database internal representation.

    Attributes:
        id (int): The unique identifier for the relation.
        subject (int): The subject entity ID.
        predicate (int): The predicate (role) ID.
        object (int): The object entity ID.
    """

    id: int
    subject: int
    predicate: int
    object: int

    def to_domain(self) -> "RelationInternal":
        """
        Converts the RelationDB instance to a RelationInternal instance.

        Returns:
            RelationInternal: The internal representation of the relation.
        """
        relation_db_dict: dict = self.model_dump()
        role_id: int = relation_db_dict.get("predicate")
        role_name = RoleCache.role_id_to_role_name_lookup[role_id]
        relation_db_dict.update(role=role_name)
        return RelationInternal.model_validate(relation_db_dict)


class Relation(_RelationBase):
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


class RelationInternal(_RelationBase):
    """
    Saved Relation representation, database internal representation.

    Attributes:
        id (int): The unique identifier for the relation.
        subject (int): The subject entity ID.
        role (str): The role of the relation.
        object (int): The object entity ID.
    """

    id: int
    subject: int
    role: str
    object: int

    def to_relation(self) -> Relation | None:
        """
        Converts the RelationInternal instance to a Relation instance.

        Returns:
            Relation | None: The public facing representation of the relation, or None if not found.
        """
        try:
            entity_one_id, entity_one_type = to_entity_external_id(self.subject)
            entity_two_id, entity_two_type = to_entity_external_id(self.object)
            return Relation(
                id=self.id,
                entity_one_id=entity_one_id,
                entity_one_type=entity_one_type,
                entity_two_id=entity_two_id,
                entity_two_type=entity_two_type,
                role=self.role,
            )
        except NotFoundError:
            return None

    @classmethod
    def to_relations(
        cls,
        relation_internals: list[Self],
    ) -> list[Relation]:
        """
        Converts a list of RelationInternal instances to a list of Relation instances.

        Args:
            relation_internals (list[Self]): A list of RelationInternal instances.

        Returns:
            list[Relation]: A list of public facing Relation instances.
        """
        relations = []
        for relation_internal in relation_internals:
            relation = relation_internal.to_relation()
            if relation:
                relations.append(relation)
        return relations


class RelationResult(Relation):
    """
    Domain Search result Relation representation, public facing.

    Attributes:
        id (int): The unique identifier for the relation.
        role (str): The role of the relation.
        distance (int | None): The distance of the relation, if applicable.
    """

    id: int
    role: str
    distance: int | None = None

    def as_json(self) -> Dict[str, Any]:
        """
        Returns the JSON representation of the relation result.

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
