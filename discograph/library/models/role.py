import logging
import re

import peewee

from discograph.library.discogs_model import DiscogsModel
from discograph.library.fields.enum_field import EnumField
from discograph.library.fields.role_type import RoleType

log = logging.getLogger(__name__)


class Role(DiscogsModel):
    # CLASS VARIABLES

    # PEEWEE FIELDS

    role_id = peewee.AutoField()
    role_name = peewee.TextField(index=False)
    role_category = EnumField(index=False, choices=RoleType.Category)
    role_subcategory = EnumField(index=False, choices=RoleType.Subcategory)
    role_category_name = peewee.TextField(index=False)
    role_subcategory_name = peewee.TextField(index=False)

    # PEEWEE META

    class Meta:
        table_name = "role"

    # PUBLIC METHODS

    @classmethod
    def loader_pass_one(cls, date: str):
        log.debug("role loader pass one")

        required_count = 0
        for role_name, categories in sorted(RoleType.role_definitions.items()):
            if categories is None or len(categories) == 0:
                continue
            role_name = Role.normalize(role_name)
            category_id = categories[0]
            category_name = RoleType.category_names[category_id]
            if len(categories) == 2:
                subcategory_id = categories[1]
                subcategory_name = RoleType.subcategory_names[subcategory_id]
            else:
                subcategory_id = RoleType.Subcategory.NONE
                subcategory_name = RoleType.subcategory_names[subcategory_id]
            log.debug(
                f"role_name: {role_name}, category_id: {category_id}, category_name: {category_name}, "
                + f"subcategory_id: {subcategory_id}, subcategory_name: {subcategory_name}"
            )
            cls.create(
                role_name=role_name,
                role_category=category_id,
                role_subcategory=subcategory_id,
                role_category_name=category_name,
                role_subcategory_name=subcategory_name,
            )
            required_count += 1

        inserted_count = cls.select().count()
        log.debug(f"inserted_count: {inserted_count}, required_count: {required_count}")
        assert required_count == inserted_count

    @classmethod
    def get_by_name(cls, name: str):
        role = cls.get(
            cls.role_name == Role.normalize(name),
        )
        return role

    @staticmethod
    def normalize(input_name: str) -> str:
        def upper(match):
            return match.group(1).upper()

        def lower(match):
            return match.group(1).lower()

        if input_name.isupper():
            return input_name

        capitalised = " ".join(
            word.capitalize() if not word.isupper() else word
            for word in input_name.split(" ")
        )
        hyphenated = re.sub(r"(-[a-z])", upper, capitalised)
        apos1 = re.sub(r"( [A-Z]')", lower, hyphenated)
        apos2 = re.sub(r"('[a-z])", upper, apos1)
        bracket = re.sub(r"(\([a-z])", upper, apos2)
        # print(f"bracket: {bracket}, apos2: {apos2}")
        normalized_name = bracket
        return normalized_name
