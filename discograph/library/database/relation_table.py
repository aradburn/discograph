from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    ForeignKeyConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.database import EntityTable
from discograph.library.database.base_table import Base
from discograph.library.database.role_table import RoleTable
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.int_enum import IntEnum


class RelationTable(Base):
    __tablename__ = "relation"

    # COLUMNS

    relation_id: Mapped[int] = mapped_column(primary_key=True)
    entity_one_id: Mapped[int] = mapped_column(Integer)
    entity_one_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    entity_two_id: Mapped[int] = mapped_column(Integer)
    entity_two_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    role_id: Mapped[int] = mapped_column(ForeignKey(RoleTable.role_id))
    random: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        ForeignKeyConstraint(
            [entity_one_id, entity_one_type],
            [EntityTable.entity_id, EntityTable.entity_type],
        ),
        ForeignKeyConstraint(
            [entity_two_id, entity_two_type],
            [EntityTable.entity_id, EntityTable.entity_type],
        ),
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

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={"random"})
