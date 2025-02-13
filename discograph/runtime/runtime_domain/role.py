__all__ = [
    "RuntimeRole",
    "RuntimeRoleJSTree",
    "RuntimeRoleJSTreeEntry",
    "RuntimeRoleJSTreeState",
    "RuntimeRoleJSTreeWrapper",
]

from dataclasses import field

from discograph.library.domain.base import InternalDomainObject
from discograph.library.fields.role_type import RoleType


class RuntimeRole(InternalDomainObject):
    id: int
    role_name: str
    role_category: RoleType.Category
    role_subcategory: RoleType.Subcategory
    role_category_name: str
    role_subcategory_name: str


class RuntimeRoleJSTreeState(InternalDomainObject):
    opened: bool  # is the node open
    disabled: bool  # is the node disabled
    selected: bool  # is the node selected


class RuntimeRoleJSTreeEntry(InternalDomainObject):
    id: str  # required
    parent: str  # required
    text: str  # node text
    icon: str | None = None  # for custom icon
    state: RuntimeRoleJSTreeState
    li_attr: dict  # attributes for the generated LI node
    a_attr: dict  # attributes for the generated A node


class RuntimeRoleJSTree(InternalDomainObject):
    data: list[RuntimeRoleJSTreeEntry] = field(default_factory=list)


class RuntimeRoleJSTreeWrapper(InternalDomainObject):
    core: RuntimeRoleJSTree
    checkbox: dict
    plugins: list[str]
