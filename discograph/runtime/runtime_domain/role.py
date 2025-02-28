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
    """
    Represents a runtime role.

    Attributes:
        id (int): The unique identifier for the role.
        role_name (str): The name of the role.
        role_category (RoleType.Category): The category of the role.
        role_subcategory (RoleType.Subcategory): The subcategory of the role.
        role_category_name (str): The name of the role category.
        role_subcategory_name (str): The name of the role subcategory.
    """

    id: int
    role_name: str
    role_category: RoleType.Category
    role_subcategory: RoleType.Subcategory
    role_category_name: str
    role_subcategory_name: str


class RuntimeRoleJSTreeState(InternalDomainObject):
    """
    Represents the state of a node in a JSTree.

    Attributes:
        opened (bool): Indicates if the node is open.
        disabled (bool): Indicates if the node is disabled.
        selected (bool): Indicates if the node is selected.
    """

    opened: bool  # is the node open
    disabled: bool  # is the node disabled
    selected: bool  # is the node selected


class RuntimeRoleJSTreeEntry(InternalDomainObject):
    """
    Represents an entry in a JSTree.

    Attributes:
        id (str): The unique identifier for the entry.
        parent (str): The parent node identifier.
        text (str): The text of the node.
        icon (str | None): The custom icon for the node.
        state (RuntimeRoleJSTreeState): The state of the node.
        li_attr (dict): The attributes for the generated LI node.
        a_attr (dict): The attributes for the generated A node.
    """

    id: str  # required
    parent: str  # required
    text: str  # node text
    icon: str | None = None  # for custom icon
    state: RuntimeRoleJSTreeState
    li_attr: dict  # attributes for the generated LI node
    a_attr: dict  # attributes for the generated A node


class RuntimeRoleJSTree(InternalDomainObject):
    """
    Represents a JSTree structure.

    Attributes:
        data (list[RuntimeRoleJSTreeEntry]): A list of JSTree entries.
    """

    data: list[RuntimeRoleJSTreeEntry] = field(default_factory=list)


class RuntimeRoleJSTreeWrapper(InternalDomainObject):
    """
    Wrapper for JSTree configuration.

    Attributes:
        core (RuntimeRoleJSTree): The core JSTree structure.
        checkbox (dict): The configuration for checkboxes.
        plugins (list[str]): A list of plugins for the JSTree.
    """

    core: RuntimeRoleJSTree
    checkbox: dict
    plugins: list[str]
