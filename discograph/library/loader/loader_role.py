import csv
import json
import logging
import os
import re
from typing import List

from discograph.config import INSTRUMENTS_PATH, INSTRUMENTS_DIR
from discograph.exceptions import NotFoundError
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.role_repository import RoleRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.instruments import HornbostelSachs
from discograph.library.domain.role import RoleUncommited
from discograph.library.fields.role_type import RoleType
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader_utils import LoaderUtils

log = logging.getLogger(__name__)


class LoaderRole(LoaderBase):
    # CLASS METHODS

    @classmethod
    def load_roles_into_database(cls) -> None:
        log.debug(f"Loading initial roles ")

        # Read from each source of roles and save into database, deduplicating role names as we go
        default_roles = LoaderRole.load_roles_from_files()
        LoaderRole.save_roles(default_roles)

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
            # print(f"instruments_data: {instruments_data}")
            for key, instrument_entry in instruments_data.root.items():
                # print(f"key: {key}")
                instrument_class_key = key[0]
                instrument_class = instruments_data.root.get(instrument_class_key).label
                category_id = RoleType.Category.INSTRUMENTS
                category_name = RoleType.category_names[category_id]
                subcategory_id = RoleType.hornbostel_sachs_to_subcategory(
                    instrument_class
                )
                subcategory_name = RoleType.subcategory_names[subcategory_id]
                # print(f"inst_class: {inst_class}")
                # print(f"inst_subcategory: {inst_subcategory}")
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
                        # log.debug(f"role_name: {role_name}")
                        # log.debug(f"category_id: {category_id}")
                        # log.debug(f"category_name: {category_name}")
                        # log.debug(f"subcategory_id: {subcategory_id}")
                        # log.debug(f"subcategory_name: {subcategory_name}")

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

        from discograph.library.cache.cache_manager import cache

        cache.clear()

        with transaction():
            added_count = 0
            role_repository = RoleRepository()

            for role_uncommitted in roles:
                try:
                    role_repository.get_by_name(name=role_uncommitted.role_name)
                    # log.debug(
                    #     f"Role record already exists in db: {role_uncommitted.role_name}"
                    # )
                except NotFoundError:
                    # Add new role
                    created_role = role_repository.create(role_uncommitted)
                    # log.debug(f"Added role to db: {created_role}")
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
            roles = role_repository.all()
            for role in roles:
                RoleDataAccess.role_id_to_role_name_lookup[role.role_id] = (
                    role.role_name
                )
                RoleDataAccess.role_id_to_role_category_lookup[role.role_id] = (
                    role.role_category
                )

        RoleDataAccess.role_name_to_role_id_lookup = {
            v: k for k, v in RoleDataAccess.role_id_to_role_name_lookup.items()
        }
        for role_name in RoleDataAccess.role_id_to_role_name_lookup.values():
            RoleDataAccess.role_name_set.add(role_name)
        # sorted_list = sorted(RoleDataAccess.role_name_set)
        # for item in sorted_list:
        #     log.debug(f"{item}")
        log.debug(
            f"Loaded {len(RoleDataAccess.role_name_to_role_id_lookup)} roles from RoleRepository"
        )

    # @classmethod
    # def load_role_words(cls) -> None:
    #     log.debug(f"Loading role words")
    #     for role_name in RoleDataAccess.role_name_set:
    #         role_name_words = role_name.split(" ")
    #         for word in role_name_words:
    #             lword = word.lower()
    #             lword_only_letters = re.sub(r"\W", "", lword)
    #             # print(f"{word} -> {lword_only_letters}")
    #             if lword_only_letters is not None and lword_only_letters != "":
    #                 RoleDataAccess.role_word_lookup.add(lword_only_letters)
    #
    #     log.debug(f"Loaded {len(RoleDataAccess.role_word_lookup)} role words")

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
