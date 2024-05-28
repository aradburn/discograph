from sqlalchemy import String, JSON, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column

from discograph.library.database.base_table import BaseTable
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.int_enum import IntEnum


class EntityTable(BaseTable):
    __tablename__ = "entity"

    # COLUMNS

    entity_id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[EntityType] = mapped_column(
        IntEnum(EntityType),
        primary_key=True,
    )
    entity_name: Mapped[str] = mapped_column(String, index=True, nullable=False)
    relation_counts: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    entity_metadata: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    entities: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    search_content: Mapped[str] = mapped_column(String, nullable=False)
    random: Mapped[float]

    __table_args__ = (
        PrimaryKeyConstraint(entity_id, entity_type),
        {},
    )
