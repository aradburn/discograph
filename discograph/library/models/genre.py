import logging
import re

from sqlalchemy import String, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Mapped, mapped_column, Session

from discograph.library.database.database_helper import Base
from discograph.library.loader_base import LoaderBase

log = logging.getLogger(__name__)


class Genre(Base, LoaderBase):
    __tablename__ = "genre"

    # COLUMNS

    genre_id: Mapped[int] = mapped_column(primary_key=True)
    genre_name: Mapped[str] = mapped_column(String)

    # CLASS VARIABLES

    # PUBLIC METHODS

    @classmethod
    def create_or_get(cls, session: Session, name: str):
        try:
            genre = session.scalar(select(Genre).where(cls.genre_name == name))
            log.debug(f"genre existing: {genre}")
        except NoResultFound:
            genre = Genre(genre_name=name)
            session.add(genre)
            session.commit()
            log.debug(f"genre created: {genre}")
        return genre

    @staticmethod
    def normalize(input_name: str) -> str:
        def upper(match):
            return match.group(1).upper()

        def lower(match):
            return match.group(1).lower()

        if input_name.isupper():
            return input_name

        capitalised = " ".join(
            word.capitalize() if not word.isupper() else word
            for word in input_name.split(" ")
        )
        hyphenated = re.sub(r"(-[a-z])", upper, capitalised)
        apos1 = re.sub(r"( [A-Z]')", lower, hyphenated)
        apos2 = re.sub(r"('[a-z])", upper, apos1)
        bracket = re.sub(r"(\([a-z])", upper, apos2)
        # print(f"bracket: {bracket}, apos2: {apos2}")
        normalized_name = bracket
        return normalized_name
