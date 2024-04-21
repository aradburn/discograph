from typing import List

from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discograph.library.database.base_table import BaseTable
from discograph.library.database.release_info_table import ReleaseInfoTable
from discograph.library.database.release_table import ReleaseTable
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
    # role: Mapped[str] = mapped_column(String, primary_key=True)
    # releases: Mapped[List["ReleaseDB"]] = relationship()
    # release_infos: Mapped[Set[ReleaseInfo]] = relationship()
    # year: Mapped[int] = mapped_column(Integer)
    # releases: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    releases: Mapped[List[ReleaseTable]] = relationship(
        # "ReleaseTable",
        secondary=ReleaseInfoTable.__table__,
        # lazy="selectin",
        # back_populates="releases",
    )

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

    # PUBLIC PROPERTIES
    # @property
    # def releases(self) -> List[ReleaseTable]:
    #     return [release_info for release_info in self.release_infos]

    # def __repr__(self):
    #     # easy to override, and it'll honor __repr__ in foreign relationships
    #     return super().__repr__(relation_id=self.relation_id, releases=self.releases)
