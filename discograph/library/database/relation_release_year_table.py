from sqlalchemy import (
    Index,
    Integer,
)
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.database.base_table import Base


class RelationReleaseYearTable(Base):
    __tablename__ = "relation_release_year"

    # COLUMNS

    relation_release_year_id: Mapped[int] = mapped_column(primary_key=True)
    relation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    release_id: Mapped[int] = mapped_column(Integer, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        Index(
            "idx_relation_release_year_relation_ids",
            relation_id,
            unique=False,
        ),
        {},
    )

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={})
