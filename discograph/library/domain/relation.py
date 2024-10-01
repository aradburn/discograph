import logging
from typing import Dict, Any

from discograph import utils
from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.entity_type import EntityType

__all__ = [
    "RelationUncommitted",
    "RelationDB",
    "Relation",
    "RelationResult",
]

log = logging.getLogger(__name__)


class _RelationBase(InternalDomainObject):
    entity_one_id: int
    entity_one_type: EntityType
    entity_two_id: int
    entity_two_type: EntityType
    # releases: Dict[str, int | None] | None = None
    random: float


class RelationUncommitted(_RelationBase):
    """This schema is used for creating an instance without an id before it is persisted into the database."""

    role_name: str


class RelationDB(_RelationBase):
    """Saved Relation representation, database internal representation."""

    relation_id: int
    # version_id: int
    role_id: int


class Relation(_RelationBase):
    """Domain Relation representation, public facing."""

    relation_id: int
    # version_id: int
    role: str
    releases: Dict[str, int | None] | None = None


class RelationResult(_RelationBase):
    """Domain Search result Relation representation, public facing."""

    relation_id: int
    role: str
    distance: int | None = None
    pages: tuple | None = None

    @property
    def entity_one_key(self) -> tuple[int, EntityType]:
        return self.entity_one_id, self.entity_one_type

    @property
    def entity_two_key(self) -> tuple[int, EntityType]:
        return self.entity_two_id, self.entity_two_type

    @property
    def json_entity_one_key(self) -> str:
        if self.entity_one_type == EntityType.ARTIST:
            return f"artist-{self.entity_one_id}"
        elif self.entity_one_type == EntityType.LABEL:
            return f"label-{self.entity_one_id}"
        raise ValueError(self.entity_one_key)

    @property
    def json_entity_two_key(self) -> str:
        if self.entity_two_type == EntityType.ARTIST:
            return f"artist-{self.entity_two_id}"
        elif self.entity_two_type == EntityType.LABEL:
            return f"label-{self.entity_two_id}"
        raise ValueError(self.entity_two_key)

    @property
    def link_key(self) -> str:
        source = self.json_entity_one_key
        target = self.json_entity_two_key
        role = utils.WORD_PATTERN.sub("-", str(self.role)).lower()
        pieces = [
            source,
            role,
            target,
        ]
        return "-".join(str(_) for _ in pieces)

    def as_json(self) -> Dict[str, Any]:
        data = {
            "key": self.link_key,
            "role": self.role,
            "source": self.json_entity_one_key,
            "target": self.json_entity_two_key,
        }
        if hasattr(self, "distance"):
            data["distance"] = self.distance
        if hasattr(self, "pages"):
            if self.pages is not None:
                pages = tuple(sorted(self.pages))
            else:
                pages = None
            data["pages"] = pages
        return data
