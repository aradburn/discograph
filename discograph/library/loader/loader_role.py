import csv
import json
import logging
import os
from typing import List

from discograph.config import INSTRUMENTS_PATH, INSTRUMENTS_DIR
from discograph.exceptions import NotFoundError
from discograph.library.cache.cache_manager import CacheManager
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.role_repository import RoleRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.instruments import HornbostelSachs
from discograph.library.domain.role import (
    RoleUncommited,
    Role,
    RoleJSTreeState,
    RoleJSTreeEntry,
)
from discograph.library.fields.role_type import RoleType
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader.loader_utils import LoaderUtils
from discograph.logging_config import LOGGING_TRACE
from discograph.ui import default_roles

log = logging.getLogger(__name__)


class LoaderRole(LoaderBase):
    # CLASS METHODS

    @classmethod
    def load_roles_into_database(cls) -> None:
        log.debug(f"Loading initial roles ")

        # Read from each source of roles and save into database, deduplicating role names as we go
        file_roles = LoaderRole.load_roles_from_files()
        LoaderRole.save_roles(file_roles)

        hornbostel_sachs_roles = LoaderRole.load_hornbostel_sachs_instruments()
        LoaderRole.save_roles(hornbostel_sachs_roles)

        wikipedia_roles = LoaderRole.load_wikipedia_instruments()
        LoaderRole.save_roles(wikipedia_roles)

        # Load back in all roles from database
        LoaderRole.load_all_roles()
        log.debug(f"Initial roles loaded OK")

    @classmethod
    def load_wikipedia_instruments(cls) -> List[RoleUncommited]:
        log.debug(f"Loading Wikipedia instruments")

        roles = []
        loaded_count = 0
        filename_list = [
            "aerophones.csv",
            "chordophones.csv",
            "electrophones.csv",
            "idiophones.csv",
            "membranophones.csv",
        ]

        # Load wikipedia data
        for filename in filename_list:
            role_path = os.path.join(INSTRUMENTS_DIR, filename)
            log.debug(f"Loading from: {role_path}")
            with open(role_path) as csvfile:
                dialect = csv.Sniffer().sniff(csvfile.read(1024))
                csvfile.seek(0)
                aerophones_csv_reader = csv.DictReader(csvfile, dialect=dialect)

                for row in aerophones_csv_reader:
                    row: dict
                    instrument_name = row["Instrument"]
                    instrument_class = row["Classification"]
                    normalised_role_name_list = RoleDataAccess.normalise_role_names(
                        instrument_name
                    )

                    for normalised_role_name in normalised_role_name_list:
                        category_id = RoleType.Category.INSTRUMENTS
                        category_name = RoleType.category_names[category_id]
                        subcategory_id = RoleType.hornbostel_sachs_to_subcategory(
                            instrument_class
                        )
                        subcategory_name = RoleType.subcategory_names[subcategory_id]
                        new_role = RoleUncommited(
                            role_name=normalised_role_name,
                            role_category=category_id,
                            role_subcategory=subcategory_id,
                            role_category_name=category_name,
                            role_subcategory_name=subcategory_name,
                        )
                        roles.append(new_role)
                        loaded_count += 1

        log.debug(f"Loaded {loaded_count} roles")
        return roles

    @classmethod
    def load_hornbostel_sachs_instruments(cls) -> List[RoleUncommited]:
        # Load Hornbostel Sachs instrument data
        log.debug(f"Load Hornbostel Sachs instrument data")

        roles = []
        loaded_count = 0

        with open(INSTRUMENTS_PATH) as f:
            json_data = json.load(f)
            instruments_data = HornbostelSachs(**json_data)
            if LOGGING_TRACE:
                print(f"instruments_data: {instruments_data}")

            for key, instrument_entry in instruments_data.root.items():
                instrument_class_key = key[0]
                instrument_class = instruments_data.root.get(instrument_class_key).label
                category_id = RoleType.Category.INSTRUMENTS
                category_name = RoleType.category_names[category_id]
                subcategory_id = RoleType.hornbostel_sachs_to_subcategory(
                    instrument_class
                )
                subcategory_name = RoleType.subcategory_names[subcategory_id]

                for instrument_name in instrument_entry.instruments:
                    normalised_role_name_list = RoleDataAccess.normalise_role_names(
                        instrument_name
                    )

                    for normalised_role_name in normalised_role_name_list:
                        new_role = RoleUncommited(
                            role_name=normalised_role_name,
                            role_category=category_id,
                            role_subcategory=subcategory_id,
                            role_category_name=category_name,
                            role_subcategory_name=subcategory_name,
                        )
                        roles.append(new_role)
                        loaded_count += 1

        log.debug(f"Loaded {loaded_count} roles")
        return roles

    @classmethod
    def load_roles_from_files(cls) -> List[RoleUncommited]:
        log.debug(f"Loading roles from files")

        roles = []
        loaded_count = 0

        role_paths = LoaderUtils.get_role_paths()
        for role_path in role_paths:
            log.debug(f"Loading from: {role_path}")
            with open(role_path, encoding="utf-8") as csvfile:
                dialect = csv.Sniffer().sniff(csvfile.read(1024))
                csvfile.seek(0)
                csv_reader = csv.DictReader(csvfile, dialect=dialect)

                for row in csv_reader:
                    row: dict
                    role_name = row["name"]
                    normalised_role_name_list = RoleDataAccess.normalise_role_names(
                        role_name
                    )

                    for normalised_role_name in normalised_role_name_list:
                        category_id = RoleType.Category[row["category"]]
                        category_name = RoleType.category_names[category_id]
                        if row["subcategory"]:
                            subcategory_id = RoleType.Subcategory[row["subcategory"]]
                        else:
                            subcategory_id = RoleType.Subcategory.NONE
                        subcategory_name = RoleType.subcategory_names[subcategory_id]
                        if LOGGING_TRACE:
                            log.debug(f"role_name: {role_name}")
                            log.debug(f"category_id: {category_id}")
                            log.debug(f"category_name: {category_name}")
                            log.debug(f"subcategory_id: {subcategory_id}")
                            log.debug(f"subcategory_name: {subcategory_name}")

                        # Add new role
                        new_role = RoleUncommited(
                            role_name=normalised_role_name,
                            role_category=category_id,
                            role_subcategory=subcategory_id,
                            role_category_name=category_name,
                            role_subcategory_name=subcategory_name,
                        )
                        roles.append(new_role)
                        loaded_count += 1

        log.debug(f"Loaded {loaded_count} roles")
        return roles

    @classmethod
    def save_roles(cls, roles: List[RoleUncommited]) -> int:
        log.debug(f"Adding roles to RoleRepository")

        CacheManager.clear()

        with transaction():
            added_count = 0
            role_repository = RoleRepository()

            for role_uncommitted in roles:
                try:
                    role_repository.get_by_name(name=role_uncommitted.role_name)
                    if LOGGING_TRACE:
                        log.debug(
                            f"Role record already exists in db: {role_uncommitted.role_name}"
                        )
                except NotFoundError:
                    # Add new role
                    created_role = role_repository.create(role_uncommitted)
                    if LOGGING_TRACE:
                        log.debug(f"Added role to db: {created_role}")
                    role_repository.commit()
                    added_count += 1

        log.debug(f"Added {added_count} roles")
        return added_count

    @classmethod
    def load_all_roles(cls) -> None:
        log.debug(f"Loading all roles from RoleRepository")

        RoleDataAccess.role_id_to_role_name_lookup.clear()
        RoleDataAccess.role_id_to_role_category_lookup.clear()
        RoleDataAccess.role_name_to_role_id_lookup.clear()
        RoleDataAccess.role_name_set.clear()

        with transaction():
            role_repository = RoleRepository()
            roles = list(role_repository.all())
            for role in roles:
                RoleDataAccess.role_id_to_role_name_lookup[role.id] = role.role_name
                RoleDataAccess.role_id_to_role_category_lookup[role.id] = (
                    role.role_category
                )

        cls.build_role_tree(roles)

        RoleDataAccess.role_name_to_role_id_lookup = {
            v: k for k, v in RoleDataAccess.role_id_to_role_name_lookup.items()
        }
        for role_name in RoleDataAccess.role_id_to_role_name_lookup.values():
            RoleDataAccess.role_name_set.add(role_name)
        if LOGGING_TRACE:
            sorted_list = sorted(RoleDataAccess.role_name_set)
            for item in sorted_list:
                log.debug(f"{item}")
        log.debug(
            f"Loaded {len(RoleDataAccess.role_name_to_role_id_lookup)} roles from RoleRepository"
        )

    @classmethod
    def build_role_tree(cls, roles: list[Role]) -> None:
        for category_name in sorted(RoleType.category_names.values()):
            # Wrap category and subcategory names to prevent clashes with actual names
            # wrapped_category_name = f"[{category_name}]"
            state = RoleJSTreeState(opened=False, disabled=False, selected=False)
            tree_entry = RoleJSTreeEntry(
                id=category_name,
                parent="#",
                text=category_name,
                icon=None,
                state=state,
                li_attr={},
                a_attr={},
            )
            # log.debug(f"tree_entry: {tree_entry.model_dump_json()}")
            RoleDataAccess.role_jstree.data.append(tree_entry)

            # Add empty array to category lookup
            RoleDataAccess.role_category_to_role_name_lookup[category_name] = []

        for subcategory_name in sorted(RoleType.subcategory_names.values()):
            if (
                subcategory_name
                is not RoleType.subcategory_names[RoleType.Subcategory.NONE]
            ):
                # Wrap category and subcategory names to prevent clashes with actual names
                # wrapped_subcategory_name = f"({subcategory_name})"
                state = RoleJSTreeState(opened=False, disabled=False, selected=False)
                tree_entry = RoleJSTreeEntry(
                    id=subcategory_name,
                    parent="Instruments",
                    text=subcategory_name,
                    icon=None,
                    state=state,
                    li_attr={},
                    a_attr={},
                )
                # log.debug(f"tree_entry: {tree_entry.model_dump_json()}")
                RoleDataAccess.role_jstree.data.append(tree_entry)

                # Add empty array to category lookup
                RoleDataAccess.role_category_to_role_name_lookup[subcategory_name] = []

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
            if role.role_name in default_roles:
                state = RoleJSTreeState(opened=False, disabled=False, selected=True)
            else:
                state = RoleJSTreeState(opened=False, disabled=False, selected=False)
            tree_entry = RoleJSTreeEntry(
                id=str(role.id),
                parent=parent,
                text=role.role_name,
                icon=None,
                state=state,
                li_attr={},
                a_attr={},
            )
            # log.debug(f"tree_entry: {tree_entry.model_dump_json()}")
            RoleDataAccess.role_jstree.data.append(tree_entry)

            RoleDataAccess.role_category_to_role_name_lookup[parent].append(
                role.role_name
            )

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
    def insert_bulk(cls, bulk_inserts, processed_count):
        pass

    @classmethod
    def update_bulk(cls, bulk_updates, processed_count):
        pass

    @classmethod
    def delete_bulk(cls, bulk_deletes, processed_count):
        pass

    @classmethod
    def get_set_of_ids(cls, entity_type):
        pass
