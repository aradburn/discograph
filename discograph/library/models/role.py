import logging
import re

from sqlalchemy import String, Enum, select
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.functions import func

from discograph.library.database.database_helper import Base
from discograph.library.fields.role_type import RoleType
from discograph.library.loader_base import LoaderBase

log = logging.getLogger(__name__)


class Role(Base, LoaderBase):
    __tablename__ = "role"

    # COLUMNS

    role_id: Mapped[int] = mapped_column(primary_key=True)
    role_name: Mapped[str] = mapped_column(String)
    role_category: Mapped[RoleType.Category] = mapped_column(Enum(RoleType.Category))
    role_subcategory: Mapped[RoleType.Subcategory] = mapped_column(
        Enum(RoleType.Subcategory)
    )
    role_category_name: Mapped[str] = mapped_column(String)
    role_subcategory_name: Mapped[str] = mapped_column(String)

    # CLASS VARIABLES

    # PUBLIC METHODS

    @classmethod
    def loader_pass_one(cls, date: str):
        from discograph.library.database.database_helper import DatabaseHelper

        log.debug("role loader pass one")

        with DatabaseHelper.session_factory() as session:
            with session.begin():
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
                    # log.debug(
                    #     f"role_name: {role_name}, category_id: {category_id}, category_name: {category_name}, "
                    #     + f"subcategory_id: {subcategory_id}, subcategory_name: {subcategory_name}"
                    # )
                    new_role = Role(
                        role_name=role_name,
                        role_category=category_id,
                        role_subcategory=subcategory_id,
                        role_category_name=category_name,
                        role_subcategory_name=subcategory_name,
                    )
                    session.add(new_role)
                    required_count += 1

                inserted_count = session.scalar(select(func.count()).select_from(Role))
                log.debug(
                    f"inserted_count: {inserted_count}, required_count: {required_count}"
                )
                assert required_count == inserted_count

    @classmethod
    def get_by_name(cls, session, name: str):
        normalized_name = Role.normalize(name)
        role = session.scalars(
            select(Role).where(Role.role_name == normalized_name)
        ).one()
        return role

    @staticmethod
    def normalize(input_name: str) -> str:
        def upper(match):
            return match.group(1).upper()

        def lower(match):
            return match.group(1).lower()

        if input_name.isupper():
            return input_name

        name = input_name.replace("-By", " By")
        name = name.replace("-by", " By")
        name = name.replace("Cgi", "CGI")
        name = name.replace("Dj", "DJ")

        if name == "Vibes":
            return "Vibraphone"
        if name == "Artwork":
            return "Artwork By"
        if name == "Artwork & Package Design By":
            return "Artwork By"

        name = " ".join(
            word.capitalize() if not word.isupper() else word
            for word in name.split(" ")
        )
        # Capitalise after hyphen
        name = re.sub(r"(-[a-z])", upper, name)
        # Do not capitalise before apostrophe
        name = re.sub(r"( [A-Z]')", lower, name)
        # Capitalise after apostrophe
        name = re.sub(r"('[a-z])", upper, name)
        # Capitalise after ampersand
        name = re.sub(r"(&[a-z])", upper, name)
        # Capitalise after round bracket
        name = re.sub(r"(\([a-z])", upper, name)
        # print(f"bracket: {bracket}, apos2: {apos2}")
        return name
