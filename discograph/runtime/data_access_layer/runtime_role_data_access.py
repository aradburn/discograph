import logging

from discograph.library.cache.role_cache import RoleCache
from discograph.library.fields.role_type import RoleType
from discograph.logging_config import LOGGING_TRACE

from discograph.runtime.runtime_database.runtime_transaction import transaction
from discograph.runtime.runtime_domain.role import (
    RuntimeRole,
    RuntimeRoleJSTreeState,
    RuntimeRoleJSTreeEntry,
)
from discograph.ui import UI_DEFAULT_ROLES

log = logging.getLogger(__name__)


class RuntimeRoleDataAccess:

    @classmethod
    def build_role_tree(cls, roles: list[RuntimeRole]) -> None:
        for category_name in sorted(RoleType.category_names.values()):
            # Wrap category and subcategory names to prevent clashes with actual names
            # wrapped_category_name = f"[{category_name}]"
            state = RuntimeRoleJSTreeState(opened=False, disabled=False, selected=False)
            tree_entry = RuntimeRoleJSTreeEntry(
                id=category_name,
                parent="#",
                text=category_name,
                icon=None,
                state=state,
                li_attr={},
                a_attr={},
            )
            # log.debug(f"tree_entry: {tree_entry.model_dump_json()}")
            RoleCache.role_jstree.data.append(tree_entry)

            # Add empty array to category lookup
            RoleCache.role_category_to_role_name_lookup[category_name] = []

        for subcategory_name in sorted(RoleType.subcategory_names.values()):
            if (
                subcategory_name
                is not RoleType.subcategory_names[RoleType.Subcategory.NONE]
            ):
                # Wrap category and subcategory names to prevent clashes with actual names
                # wrapped_subcategory_name = f"({subcategory_name})"
                state = RuntimeRoleJSTreeState(
                    opened=False, disabled=False, selected=False
                )
                tree_entry = RuntimeRoleJSTreeEntry(
                    id=subcategory_name,
                    parent="Instruments",
                    text=subcategory_name,
                    icon=None,
                    state=state,
                    li_attr={},
                    a_attr={},
                )
                # log.debug(f"tree_entry: {tree_entry.model_dump_json()}")
                RoleCache.role_jstree.data.append(tree_entry)

                # Add empty array to category lookup
                RoleCache.role_category_to_role_name_lookup[subcategory_name] = []

        for role in sorted(
            roles,
            key=lambda k: (
                k.model_dump()["role_category_name"],
                k.model_dump()["role_subcategory_name"],
                k.model_dump()["role_name"],
            ),
        ):
            if role.role_subcategory is not RoleType.Subcategory.NONE:
                parent = role.role_subcategory_name
            else:
                parent = role.role_category_name
            # log.debug(f"role: {role}")
            # log.debug(f"parent: {parent}")

            # Preselect the roles in default_roles
            if role.role_name in UI_DEFAULT_ROLES:
                state = RuntimeRoleJSTreeState(
                    opened=False, disabled=False, selected=True
                )
            else:
                state = RuntimeRoleJSTreeState(
                    opened=False, disabled=False, selected=False
                )
            tree_entry = RuntimeRoleJSTreeEntry(
                id=str(role.id),
                parent=parent,
                text=role.role_name,
                icon=None,
                state=state,
                li_attr={},
                a_attr={},
            )
            # log.debug(f"tree_entry: {tree_entry.model_dump_json()}")
            RoleCache.role_jstree.data.append(tree_entry)

            RoleCache.role_category_to_role_name_lookup[parent].append(role.role_name)

            # if role.role_category_name not in RoleDataAccess.role_tree:
            #     RoleDataAccess.role_tree[role.role_category_name] = {}
            # if (
            #     role.role_subcategory_name
            #     not in RoleDataAccess.role_tree[role.role_category_name]
            # ):
            #     RoleDataAccess.role_tree[role.role_category_name][
            #         role.role_subcategory_name
            #     ] = []
            # RoleDataAccess.role_tree[role.role_category_name][
            #     role.role_subcategory_name
            # ].append(role.role_name)
        # log.debug(f"role jstree: {RoleDataAccess.role_jstree}")
        # log.debug(
        #     f"role category lookup: {RoleDataAccess.role_category_to_role_name_lookup}"
        # )

    @classmethod
    def load_all_roles(cls) -> None:
        from discograph.runtime.runtime_database.runtime_role_repository import (
            RuntimeRoleRepository,
        )

        log.info(f"Loading all roles from runtime database")

        RoleCache.role_id_to_role_name_lookup.clear()
        RoleCache.role_id_to_role_category_lookup.clear()
        RoleCache.role_name_to_role_id_lookup.clear()
        RoleCache.role_name_set.clear()

        with transaction():
            role_repository = RuntimeRoleRepository()
            roles = list(role_repository.all())
            for role in roles:
                RoleCache.role_id_to_role_name_lookup[role.id] = role.role_name
                RoleCache.role_id_to_role_category_lookup[role.id] = role.role_category

        cls.build_role_tree(roles)

        RoleCache.role_name_to_role_id_lookup = {
            v: k for k, v in RoleCache.role_id_to_role_name_lookup.items()
        }
        for role_name in RoleCache.role_id_to_role_name_lookup.values():
            RoleCache.role_name_set.add(role_name)
        if LOGGING_TRACE:
            sorted_list = sorted(RoleCache.role_name_set)
            for item in sorted_list:
                log.debug(f"{item}")
        log.debug(
            f"Loaded {len(RoleCache.role_name_to_role_id_lookup)} roles from RoleRepository"
        )
