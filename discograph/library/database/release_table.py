from sqlalchemy import String, JSON, Integer, Float, Date
from sqlalchemy.orm import Mapped, mapped_column

from discograph.library.database.base_table import BaseTable


class ReleaseTable(BaseTable):
    __tablename__ = "release"

    # COLUMNS

    release_id: Mapped[int] = mapped_column(primary_key=True)
    artists: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    companies: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    country: Mapped[str] = mapped_column(String, nullable=True)
    extra_artists: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    formats: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    genres: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    identifiers: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    labels: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    master_id: Mapped[int] = mapped_column(Integer, nullable=True)
    notes: Mapped[str] = mapped_column(String, nullable=True)
    release_date: Mapped[Date] = mapped_column(Date, nullable=True)
    styles: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    title: Mapped[str] = mapped_column(String, nullable=True)
    tracklist: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    random: Mapped[float] = mapped_column(Float)
