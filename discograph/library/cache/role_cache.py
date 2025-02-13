from typing import Dict, Set

from discograph.library.fields.role_type import RoleType
from discograph.runtime.runtime_domain.role import RuntimeRoleJSTree


class RoleCache:

    # CLASS VARIABLES
    role_name_to_role_id_lookup: Dict[str, int] = {}
    role_name_set: Set[str] = set()
    role_id_to_role_category_lookup: Dict[int, RoleType.Category] = {}
    role_id_to_role_name_lookup: Dict[int, str] = {}
    role_jstree: RuntimeRoleJSTree = RuntimeRoleJSTree()
    role_category_to_role_name_lookup: Dict[str, list[str]] = {}
    # role_categories: Set[str] = set()

    @staticmethod
    def get_all_roles() -> dict:
        roles = []
        for role_id, role_name in RoleCache.role_id_to_role_name_lookup.items():
            role_category = RoleCache.role_id_to_role_category_lookup[role_id]
            role = {
                "id": role_id,
                "role_name": role_name,
                "role_category": role_category.name,
            }
            roles.append(role)
        data = {"roles": roles}
        return data
