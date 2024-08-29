from sqlalchemy import String, Enum
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.database.database_helper import Base
from discograph.library.fields.role_type import RoleType


class RoleTable(Base):
    __tablename__ = "role"

    # COLUMNS

    role_id: Mapped[int] = mapped_column(primary_key=True)
    role_name: Mapped[str] = mapped_column(String, index=True, nullable=False)
    role_category: Mapped[RoleType.Category] = mapped_column(Enum(RoleType.Category))
    role_subcategory: Mapped[RoleType.Subcategory] = mapped_column(
        Enum(RoleType.Subcategory)
    )
    role_category_name: Mapped[str] = mapped_column(String)
    role_subcategory_name: Mapped[str] = mapped_column(String)

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={"random"})
