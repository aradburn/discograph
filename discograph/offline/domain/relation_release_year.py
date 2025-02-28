import logging

from discograph.library.domain.base import InternalDomainObject

__all__ = [
    "RelationReleaseYearUncommitted",
    "RelationReleaseYearDB",
    "RelationReleaseYear",
]

log = logging.getLogger(__name__)


class _RelationReleaseYearBase(InternalDomainObject):
    """
    Base class for relation release year entities.

    Attributes:
        relation_id (int): The ID of the relation.
        release_id (int): The ID of the release.
        year (int | None): The release year, if available.
    """

    relation_id: int
    release_id: int
    year: int | None = None


class RelationReleaseYearUncommitted(_RelationReleaseYearBase):
    """
    This schema is used for creating an instance without an id before it is persisted into the database.
    """

    pass


class RelationReleaseYearDB(_RelationReleaseYearBase):
    """
    Saved RelationReleaseYear representation, database internal representation.

    Attributes:
        relation_release_year_id (int): The unique identifier for the relation release year.
    """

    relation_release_year_id: int

    def to_domain(self) -> "RelationReleaseYear":
        """
        Converts the RelationReleaseYearDB instance to a RelationReleaseYear instance.

        Returns:
            RelationReleaseYear: The domain representation of the relation release year.
        """
        relation_release_year_db_dict: dict = self.model_dump()
        return RelationReleaseYear.model_validate(relation_release_year_db_dict)


class RelationReleaseYear(_RelationReleaseYearBase):
    """
    Domain RelationReleaseYear representation, public facing.

    Attributes:
        relation_release_year_id (int): The unique identifier for the relation release year.
    """

    relation_release_year_id: int
