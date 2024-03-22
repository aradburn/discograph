import logging
import re

import peewee

from discograph.library.discogs_model import DiscogsModel

log = logging.getLogger(__name__)


class Genre(DiscogsModel):
    # CLASS VARIABLES

    # PEEWEE FIELDS

    genre_id = peewee.AutoField()
    genre_name = peewee.TextField()

    # PEEWEE META

    class Meta:
        table_name = "genre"

    # PUBLIC METHODS

    @classmethod
    def create_or_get(cls, name: str):
        try:
            genre = cls.select().where(cls.genre_name == name).get()
            log.debug(f"genre existing: {genre}")
        except peewee.DoesNotExist:
            genre = cls.create(genre_name=name)
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
