import csv
import json
import logging
import os
from typing import Dict

from discograph.config import INSTRUMENTS_PATH, INSTRUMENTS_DIR
from discograph.exceptions import NotFoundError
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.role_repository import RoleRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.instruments import HornbostelSachs, Instrument
from discograph.library.domain.role import RoleUncommited
from discograph.library.fields.role_type import RoleType
from discograph.library.loader.loader_base import LoaderBase

log = logging.getLogger(__name__)


class LoaderRole(LoaderBase):
    # CLASS METHODS

    @classmethod
    def load_instruments(cls) -> int:
        log.debug(f"Loading instrumrnts")
        # Load wiki data
        with open(os.path.join(INSTRUMENTS_DIR, "aerophones.csv")) as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            aerophones_csv = csv.DictReader(csvfile, dialect=dialect)
            # aerophones_csv = csv.reader(csvfile, delimiter=",")

            for row in aerophones_csv:
                row_dict: Dict[str] = row
                print(row_dict)
                name = row_dict["Instrument"]
                print(f"{name}")
                # inst_subcategory = RoleType.hornbostel_sachs_to_subcategory(inst_class)
                #
                # print(f"{row['Instrument']}")

        # Load data
        with open(INSTRUMENTS_PATH) as f:
            json_data = json.load(f)
            instruments_data = HornbostelSachs(**json_data)
            # print(f"instruments_data: {instruments_data}")
        for key, instrument in instruments_data.root.items():
            # print(f"key: {key}")
            inst_class_key = key[0]
            inst_class = instruments_data.root.get(inst_class_key).label
            inst_category = RoleType.Category.INSTRUMENTS
            inst_subcategory = RoleType.hornbostel_sachs_to_subcategory(inst_class)
            # print(f"inst_class: {inst_class}")
            # print(f"inst_subcategory: {inst_subcategory}")
            for inst in instrument.instruments:
                inst_name = RoleDataAccess.normalize(inst)
                # print(f"inst: {inst_name}, {inst_subcategory}")
                # Check against defaults
                for role_name, categories in sorted(RoleType.role_definitions.items()):
                    if categories is None or len(categories) == 0:
                        continue
                    role_name = RoleDataAccess.normalize(role_name)
                    category_id = categories[0]
                    category_name = RoleType.category_names[category_id]
                    if len(categories) == 2:
                        subcategory_id = categories[1]
                        subcategory_name = RoleType.subcategory_names[subcategory_id]
                    else:
                        subcategory_id = RoleType.Subcategory.NONE
                        subcategory_name = RoleType.subcategory_names[subcategory_id]
                    if role_name.lower() == inst_name.lower():
                        if subcategory_id != inst_subcategory:
                            print(
                                f"diff category: {role_name} def: {subcategory_id} -> {inst_subcategory}"
                            )
        return len(instruments_data.model_dump())

    @classmethod
    def load_default_roles(cls) -> int:
        log.debug(f"Loading default roles")

        RoleType.role_id_to_role_name_lookup.clear()
        RoleType.role_name_to_role_id_lookup.clear()

        with transaction():
            required_count = 0
            role_repository = RoleRepository()
            for role_name, categories in sorted(RoleType.role_definitions.items()):
                if categories is None or len(categories) == 0:
                    continue
                role_name = RoleDataAccess.normalize(role_name)
                category_id = categories[0]
                category_name = RoleType.category_names[category_id]
                if len(categories) == 2:
                    subcategory_id = categories[1]
                    subcategory_name = RoleType.subcategory_names[subcategory_id]
                else:
                    subcategory_id = RoleType.Subcategory.NONE
                    subcategory_name = RoleType.subcategory_names[subcategory_id]

                try:
                    existing_role = role_repository.get_by_name(name=role_name)
                    log.debug(
                        f"Role record already exists: {role_name} {existing_role.role_name}"
                    )
                except NotFoundError:
                    # Add new role
                    new_role = RoleUncommited(
                        role_name=role_name,
                        role_category=category_id,
                        role_subcategory=subcategory_id,
                        role_category_name=category_name,
                        role_subcategory_name=subcategory_name,
                    )
                    role_repository.create(new_role)

                required_count += 1
                RoleType.role_id_to_role_name_lookup[required_count] = role_name

            record_count = role_repository.count()
            log.debug(f"record_count: {record_count}, required_count: {required_count}")
            RoleType.role_name_to_role_id_lookup = {
                v: k for k, v in RoleType.role_id_to_role_name_lookup.items()
            }
            assert required_count == record_count
        return record_count

    @classmethod
    def load_all_roles(cls) -> None:
        log.debug(f"Loading all roles")

        RoleType.role_id_to_role_name_lookup.clear()
        RoleType.role_name_to_role_id_lookup.clear()

        with transaction():
            role_repository = RoleRepository()
            roles = role_repository.all()
            for role in roles:
                RoleType.role_id_to_role_name_lookup[role.role_id] = role.role_name

            RoleType.role_name_to_role_id_lookup = {
                v: k for k, v in RoleType.role_id_to_role_name_lookup.items()
            }
        # No roles in database yet, so load in default roles
        if len(RoleType.role_id_to_role_name_lookup) == 0:
            cls.load_default_roles()

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
