import logging

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
    def loader_pass_one(cls, date: str) -> int:
        log.debug("role loader pass one")

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

            inserted_count = role_repository.count()
            log.debug(
                f"inserted_count: {inserted_count}, required_count: {required_count}"
            )
            RoleType.role_name_to_role_id_lookup = {
                v: k for k, v in RoleType.role_id_to_role_name_lookup.items()
            }
            assert required_count == inserted_count
        return inserted_count
