import logging

from discograph.exceptions import NotFoundError
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.role_repository import RoleRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.role import RoleUncommited
from discograph.library.fields.role_type import RoleType
from discograph.library.loader.loader_base import LoaderBase

log = logging.getLogger(__name__)


class LoaderRole(LoaderBase):
    # CLASS METHODS

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
                # log.debug(
                #     f"role_name: {role_name}, category_id: {category_id}, category_name: {category_name}, "
                #     + f"subcategory_id: {subcategory_id}, subcategory_name: {subcategory_name}"
                # )

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
