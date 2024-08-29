import csv
import json
import logging
import os

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
    def load_initial_roles(cls) -> None:
        log.debug(f"Loading initial roles ")

        # No roles in database yet, so load in default roles
        if len(RoleDataAccess.role_id_to_role_name_lookup) == 0:
            LoaderRole.load_roles_from_files()
            LoaderRole.load_hornbostel_sachs_instruments()
            LoaderRole.load_wikipedia_instruments()
            LoaderRole.load_all_roles()

    @classmethod
    def load_wikipedia_instruments(cls) -> int:
        log.debug(f"Loading Wikipedia instruments")

        with transaction():
            added_count = 0
            role_repository = RoleRepository()

            from discograph.library.cache.cache_manager import cache

            cache.clear()

            # Load wiki data
            with open(os.path.join(INSTRUMENTS_DIR, "aerophones.csv")) as csvfile:
                dialect = csv.Sniffer().sniff(csvfile.read(1024))
                csvfile.seek(0)
                aerophones_csv_reader = csv.DictReader(csvfile, dialect=dialect)

                for row in aerophones_csv_reader:
                    row: dict
                    instrument_name = row["Instrument"]
                    instrument_class = row["Classification"]
                    normalised_role_name = RoleDataAccess.normalise_role_name(
                        instrument_name
                    )
                    category_id = RoleType.Category.INSTRUMENTS
                    category_name = RoleType.category_names[category_id]
                    subcategory_id = RoleType.hornbostel_sachs_to_subcategory(
                        instrument_class
                    )
                    subcategory_name = RoleType.subcategory_names[subcategory_id]
                    try:
                        existing_role = role_repository.get_by_name(
                            name=normalised_role_name
                        )
                        log.debug(
                            f"Role record already exists in db: {normalised_role_name} [{instrument_name}]"
                        )
                    except NotFoundError:
                        # Add new role
                        new_role = RoleUncommited(
                            role_name=normalised_role_name,
                            role_category=category_id,
                            role_subcategory=subcategory_id,
                            role_category_name=category_name,
                            role_subcategory_name=subcategory_name,
                        )
                        created_role = role_repository.create(new_role)
                        role_repository.commit()
                        added_count += 1

        log.debug(f"Added {added_count} roles")
        return added_count

    @classmethod
    def load_hornbostel_sachs_instruments(cls) -> int:
        # Load Hornbostel Sachs instrument data

        with transaction():
            added_count = 0
            role_repository = RoleRepository()

            from discograph.library.cache.cache_manager import cache

            cache.clear()

            with open(INSTRUMENTS_PATH) as f:
                json_data = json.load(f)
                instruments_data = HornbostelSachs(**json_data)
                # print(f"instruments_data: {instruments_data}")
                for key, instrument_entry in instruments_data.root.items():
                    # print(f"key: {key}")
                    instrument_class_key = key[0]
                    instrument_class = instruments_data.root.get(
                        instrument_class_key
                    ).label
                    category_id = RoleType.Category.INSTRUMENTS
                    category_name = RoleType.category_names[category_id]
                    subcategory_id = RoleType.hornbostel_sachs_to_subcategory(
                        instrument_class
                    )
                    subcategory_name = RoleType.subcategory_names[subcategory_id]
                    # print(f"inst_class: {inst_class}")
                    # print(f"inst_subcategory: {inst_subcategory}")
                    for instrument_name in instrument_entry.instruments:
                        normalised_role_name = RoleDataAccess.normalise_role_name(
                            instrument_name
                        )
                        try:
                            existing_role = role_repository.get_by_name(
                                name=normalised_role_name
                            )
                            log.debug(
                                f"Role record already exists in db: {normalised_role_name} [{instrument_name}]"
                            )
                        except NotFoundError:
                            # Add new role
                            new_role = RoleUncommited(
                                role_name=normalised_role_name,
                                role_category=category_id,
                                role_subcategory=subcategory_id,
                                role_category_name=category_name,
                                role_subcategory_name=subcategory_name,
                            )
                            created_role = role_repository.create(new_role)
                            role_repository.commit()
                            added_count += 1

        log.debug(f"Added {added_count} roles")
        return added_count

    @classmethod
    def load_roles_from_files(cls) -> int:
        log.debug(f"Loading roles from files")

        with transaction():
            added_count = 0
            role_repository = RoleRepository()
            from discograph.library.cache.cache_manager import cache

            cache.clear()

            role_paths = LoaderUtils.get_role_paths()
            for role_path in role_paths:
                log.debug(f"role_path: {role_path}")
                with open(role_path, encoding="unicode-escape") as csvfile:
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
                                subcategory_id = RoleType.Subcategory[
                                    row["subcategory"]
                                ]
                            else:
                                subcategory_id = RoleType.Subcategory.NONE
                            subcategory_name = RoleType.subcategory_names[
                                subcategory_id
                            ]
                            # log.debug(f"role_name: {role_name}")
                            # log.debug(f"category_id: {category_id}")
                            # log.debug(f"category_name: {category_name}")
                            # log.debug(f"subcategory_id: {subcategory_id}")
                            # log.debug(f"subcategory_name: {subcategory_name}")

                            try:
                                existing_role = role_repository.get_by_name(
                                    name=normalised_role_name
                                )
                                log.debug(
                                    f"Role record already exists in db: {normalised_role_name} [{role_name}]"
                                )
                            except NotFoundError:
                                # Add new role
                                new_role = RoleUncommited(
                                    role_name=normalised_role_name,
                                    role_category=category_id,
                                    role_subcategory=subcategory_id,
                                    role_category_name=category_name,
                                    role_subcategory_name=subcategory_name,
                                )
                                created_role = role_repository.create(new_role)
                                role_repository.commit()
                                added_count += 1
        log.debug(f"Added {added_count} roles")
        return added_count

    # @classmethod
    # def load_default_roles(cls) -> int:
    #     log.debug(f"Loading default roles")
    #
    #     RoleDataAccess.role_id_to_role_name_lookup.clear()
    #     RoleDataAccess.role_name_to_role_id_lookup.clear()
    #
    #     with transaction():
    #         required_count = 0
    #         role_repository = RoleRepository()
    #         for role_name, categories in sorted(RoleType.role_definitions.items()):
    #             if categories is None or len(categories) == 0:
    #                 continue
    #             role_name = RoleDataAccess.normalize(role_name)
    #             category_id = categories[0]
    #             category_name = RoleType.category_names[category_id]
    #             if len(categories) == 2:
    #                 subcategory_id = categories[1]
    #                 subcategory_name = RoleType.subcategory_names[subcategory_id]
    #             else:
    #                 subcategory_id = RoleType.Subcategory.NONE
    #                 subcategory_name = RoleType.subcategory_names[subcategory_id]
    #
    #             try:
    #                 existing_role = role_repository.get_by_name(name=role_name)
    #                 log.debug(
    #                     f"Role record already exists: {role_name} {existing_role.role_name}"
    #                 )
    #             except NotFoundError:
    #                 # Add new role
    #                 new_role = RoleUncommited(
    #                     role_name=role_name,
    #                     role_category=category_id,
    #                     role_subcategory=subcategory_id,
    #                     role_category_name=category_name,
    #                     role_subcategory_name=subcategory_name,
    #                 )
    #                 role_repository.create(new_role)
    #
    #             required_count += 1
    #             RoleDataAccess.role_id_to_role_name_lookup[required_count] = role_name
    #
    #         record_count = role_repository.count()
    #         log.debug(f"record_count: {record_count}, required_count: {required_count}")
    #         RoleType.role_name_to_role_id_lookup = {
    #             v: k for k, v in RoleDataAccess.role_id_to_role_name_lookup.items()
    #         }
    #         assert required_count == record_count
    #     return record_count

    @classmethod
    def load_all_roles(cls) -> None:
        log.debug(f"Loading all roles")

        RoleDataAccess.role_id_to_role_name_lookup.clear()
        RoleDataAccess.role_id_to_role_category_lookup.clear()
        RoleDataAccess.role_name_to_role_id_lookup.clear()

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
