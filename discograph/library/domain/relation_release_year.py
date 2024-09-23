import logging

from discograph.library.domain.base import InternalDomainObject

__all__ = [
    "RelationReleaseYearUncommitted",
    "RelationReleaseYearDB",
    "RelationReleaseYear",
]

log = logging.getLogger(__name__)


class _RelationReleaseYearBase(InternalDomainObject):
    relation_id: int
    release_id: int
    year: int | None = None


class RelationReleaseYearUncommitted(_RelationReleaseYearBase):
    """This schema is used for creating an instance without an id before it is persisted into the database."""

    pass


class RelationReleaseYearDB(_RelationReleaseYearBase):
    """Saved RelationReleaseYear representation, database internal representation."""

    relation_release_year_id: int
    pass


class RelationReleaseYear(_RelationReleaseYearBase):
    """Domain RelationReleaseYear representation, public facing."""

    relation_release_year_id: int
    pass
