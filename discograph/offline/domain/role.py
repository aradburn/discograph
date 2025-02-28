__all__ = [
    "RoleUncommitted",
    "Role",
]

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.role_type import RoleType


class _RoleBase(InternalDomainObject):
    """
    Base class for role entities.

    Attributes:
        role_name (str): The name of the role.
        role_category (RoleType.Category): The category of the role.
        role_subcategory (RoleType.Subcategory): The subcategory of the role.
        role_category_name (str): The name of the role category.
        role_subcategory_name (str): The name of the role subcategory.
    """

    role_name: str
    role_category: RoleType.Category
    role_subcategory: RoleType.Subcategory
    role_category_name: str
    role_subcategory_name: str


class RoleUncommitted(_RoleBase):
    """
    This schema is used for creating an instance without an id before it is persisted into the database.
    """

    pass


class Role(_RoleBase):
    """
    Saved Role representation.

    Attributes:
        id (int): The unique identifier for the role.
    """

    id: int
