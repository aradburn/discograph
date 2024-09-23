from datetime import datetime

from sqlalchemy import String, TIMESTAMP, Integer, Index
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.database.base_table import Base


class MetadataTable(Base):
    __tablename__ = "metadata"

    # COLUMNS
    metadata_id: Mapped[int] = mapped_column(primary_key=True)
    version_id: Mapped[int] = mapped_column(Integer, nullable=False)
    metadata_key: Mapped[str] = mapped_column(String, nullable=False)
    metadata_value: Mapped[str] = mapped_column(String, nullable=False)
    metadata_timestamp: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)

    __mapper_args__ = {"version_id_col": version_id}

    __table_args__ = (
        Index(
            "idx_metadata",
            metadata_key,
            unique=True,
        ),
        {},
    )

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={"random"})
