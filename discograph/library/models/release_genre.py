import logging

import peewee

from discograph.library.discogs_model import DiscogsModel
from discograph.library.models.genre import Genre
from discograph.library.models.release import Release

log = logging.getLogger(__name__)


class ReleaseGenre(DiscogsModel):
    # CLASS VARIABLES

    # PEEWEE FIELDS

    release_id = peewee.ForeignKeyField(Release, field=Release.release_id)
    genre_id = peewee.ForeignKeyField(Genre, field=Genre.genre_id)

    # PEEWEE META

    class Meta:
        table_name = "release_genre"
        primary_key = peewee.CompositeKey("release_id", "genre_id")

    # PUBLIC METHODS
