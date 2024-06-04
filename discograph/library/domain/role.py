__all__ = [
    "RoleUncommited",
    "Role",
]

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.role_type import RoleType


class _RoleBase(InternalDomainObject):
    role_name: str
    role_category: RoleType.Category
    role_subcategory: RoleType.Subcategory
    role_category_name: str
    role_subcategory_name: str


class RoleUncommited(_RoleBase):
    """This schema is used for creating an instance without an id before it is persisted into the database."""

    pass


class Role(_RoleBase):
    """Saved Role representation."""

    role_id: int
