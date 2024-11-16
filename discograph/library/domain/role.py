__all__ = [
    "RoleUncommited",
    "Role",
    "RoleJSTree",
    "RoleJSTreeEntry",
    "RoleJSTreeState",
    "RoleJSTreeWrapper",
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

    id: int


class RoleJSTreeState(InternalDomainObject):
    opened: bool  # is the node open
    disabled: bool  # is the node disabled
    selected: bool  # is the node selected


class RoleJSTreeEntry(InternalDomainObject):
    id: str  # required
    parent: str  # required
    text: str  # node text
    icon: str | None = None  # for custom icon
    state: RoleJSTreeState
    li_attr: dict  # attributes for the generated LI node
    a_attr: dict  # attributes for the generated A node


class RoleJSTree(InternalDomainObject):
    data: list[RoleJSTreeEntry] = []


class RoleJSTreeWrapper(InternalDomainObject):
    core: RoleJSTree
    checkbox: dict
    plugins: list[str]
