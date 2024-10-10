from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
)
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.database.base_table import Base
from discograph.library.database.role_table import RoleTable


class RelationTable(Base):
    __tablename__ = "relation"

    # COLUMNS

    id: Mapped[int] = mapped_column(primary_key=True)
    # relation_id: Mapped[int] = mapped_column(primary_key=True)
    subject: Mapped[int] = mapped_column(Integer)
    # subject: Mapped[int] = mapped_column(ForeignKey(EntityTable.id))
    predicate: Mapped[int] = mapped_column(ForeignKey(RoleTable.id))
    object: Mapped[int] = mapped_column(Integer)
    # object: Mapped[int] = mapped_column(ForeignKey(EntityTable.id))
    # entity_one_id: Mapped[int] = mapped_column(Integer)
    # entity_one_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    # entity_two_id: Mapped[int] = mapped_column(Integer)
    # entity_two_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType))
    # role_id: Mapped[int] = mapped_column(ForeignKey(RoleTable.role_id))
    random: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        Index(
            "idx_relation",
            subject,
            predicate,
            object,
            unique=True,
        ),
        Index(
            "idx_relation_subject",
            subject,
            unique=False,
        ),
        Index(
            "idx_relation_object",
            object,
            unique=False,
        ),
        {},
    )

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={"random"})
