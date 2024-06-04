from datetime import datetime

from sqlalchemy import String, TIMESTAMP, Integer
from sqlalchemy.orm import Mapped, mapped_column

from discograph.library.database.base_table import BaseTable


class MetadataTable(BaseTable):
    __tablename__ = "metadata"

    # COLUMNS
    load_timestamp: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)
    load_stage: Mapped[str] = mapped_column(String, nullable=False)
    load_block: Mapped[int] = mapped_column(Integer, nullable=False)
