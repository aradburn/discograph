from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    UniqueConstraint,
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
    entity_one_id: Mapped[int] = mapped_column(Integer)
    entity_one_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    entity_two_id: Mapped[int] = mapped_column(Integer)
    entity_two_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    role_id: Mapped[int] = mapped_column(ForeignKey("role.role_id"))
    role: Mapped[RoleTable] = relationship()
    releases: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)

    random: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        UniqueConstraint(
            entity_one_id,
            entity_one_type,
            entity_two_id,
            entity_two_type,
            role_id,
        ),
        Index(
            "idx_entity_one_id",
            entity_one_id,
            postgresql_using="hash",
            unique=False,
        ),
        Index(
            "idx_entity_two_id",
            entity_two_id,
            postgresql_using="hash",
            unique=False,
        ),
        {},
    )
