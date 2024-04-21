from sqlalchemy import DateTime, Index, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from discograph.library.database.base_table import BaseTable


class ReleaseInfoTable(BaseTable):
    __tablename__ = "releaseinfo"

    # COLUMNS

    relation_id: Mapped[int] = mapped_column(
        ForeignKey("relation.relation_id"), primary_key=True
    )
    release_id: Mapped[int] = mapped_column(
        ForeignKey("release.release_id"), primary_key=True
    )
    release_date: Mapped[DateTime] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index(
            "idx_release_id",
            release_id,
            unique=False,
        ),
        {},
    )
