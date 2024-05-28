from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discograph.library.database.base_table import BaseTable
from discograph.library.database.role_table import RoleTable
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.int_enum import IntEnum


class RelationTable(BaseTable):
    __tablename__ = "relation"

    # COLUMNS

    relation_id: Mapped[int] = mapped_column(primary_key=True)
    version_id = mapped_column(Integer, nullable=False)
    entity_one_id: Mapped[int] = mapped_column(Integer)
    entity_one_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    entity_two_id: Mapped[int] = mapped_column(Integer)
    entity_two_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    role_id: Mapped[int] = mapped_column(ForeignKey("role.role_id"))
    role: Mapped[RoleTable] = relationship()
    releases: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)

    random: Mapped[float] = mapped_column(Float)

    __mapper_args__ = {"version_id_col": version_id}

    __table_args__ = (
        Index(
            "idx_relation",
            entity_one_id,
            entity_one_type,
            entity_two_id,
            entity_two_type,
            role_id,
            unique=True,
        ),
        Index(
            "idx_relation_one",
            entity_one_id,
            entity_one_type,
            unique=False,
        ),
        Index(
            "idx_relation_two",
            entity_two_id,
            entity_two_type,
            unique=False,
        ),
        {},
    )
