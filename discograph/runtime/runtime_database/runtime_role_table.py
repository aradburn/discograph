from sqlalchemy import String, Enum, inspect
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.fields.role_type import RoleType
from discograph.runtime.runtime_database.runtime_base_table import RuntimeBase


class RuntimeRoleTable(RuntimeBase):
    __tablename__ = "runtime_role"

    # COLUMNS

    id: Mapped[int] = mapped_column(primary_key=True)
    role_name: Mapped[str] = mapped_column(String, index=True, nullable=False)
    role_category: Mapped[RoleType.Category] = mapped_column(
        Enum(RoleType.Category, name="runtime_role_category")
    )
    role_subcategory: Mapped[RoleType.Subcategory] = mapped_column(
        Enum(RoleType.Subcategory, name="runtime_role_subcategory")
    )
    role_category_name: Mapped[str] = mapped_column(String)
    role_subcategory_name: Mapped[str] = mapped_column(String)

    def __init__(self, **entries):
        """Override to avoid TypeError when passed spurious column names"""
        column_names = set(
            [column.name for column in inspect(RuntimeRoleTable).columns]
        )
        superentries = {
            k: entries[k] for k in column_names.intersection(entries.keys())
        }
        super().__init__(**superentries)

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={})
