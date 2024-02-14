import logging

import peewee
from playhouse import postgres_ext

from discograph.library.models.release import Release


log = logging.getLogger(__name__)


class PostgresRelease(Release):
    # PEEWEE FIELDS

    id = peewee.IntegerField(primary_key=True)
    artists = postgres_ext.BinaryJSONField(null=True, index=False)
    companies = postgres_ext.BinaryJSONField(null=True, index=False)
    country = peewee.TextField(null=True, index=False)
    extra_artists = postgres_ext.BinaryJSONField(null=True, index=False)
    formats = postgres_ext.BinaryJSONField(null=True, index=False)
    genres = postgres_ext.ArrayField(peewee.TextField, null=True, index=False)
    identifiers = postgres_ext.BinaryJSONField(null=True, index=False)
    labels = postgres_ext.BinaryJSONField(null=True, index=False)
    master_id = peewee.IntegerField(null=True, index=False)
    notes = peewee.TextField(null=True, index=False)
    release_date = peewee.DateTimeField(null=True, index=False)
    styles = postgres_ext.ArrayField(peewee.TextField, null=True, index=False)
    title = peewee.TextField(index=False)
    tracklist = postgres_ext.BinaryJSONField(null=True, index=False)
