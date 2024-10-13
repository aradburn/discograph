from sqlalchemy import String, JSON, Index, Integer
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.database.base_table import Base
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.int_enum import IntEnum


class EntityTable(Base):
    __tablename__ = "entity"

    # COLUMNS

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_id: Mapped[int] = mapped_column(Integer)
    entity_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType), nullable=False)
    entity_name: Mapped[str] = mapped_column(String, index=True, nullable=False)
    relation_counts: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    entity_metadata: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    entities: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    search_content: Mapped[str] = mapped_column(String, nullable=False)
    random: Mapped[float]

    __table_args__ = (
        Index(
            "idx_entity_id_and_entity_type",
            entity_id,
            entity_type,
            unique=True,
        ),
        Index(
            "idx_entity_name_and_entity_type",
            entity_name,
            entity_type,
            unique=False,
        ),
        {},
    )

    # __table_args__ = (
    #     PrimaryKeyConstraint(entity_id, entity_type),
    #     {},
    # )

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={"random"})
