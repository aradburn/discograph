import logging

import peewee
from playhouse import sqlite_ext

from discograph.library.models.release import Release


log = logging.getLogger(__name__)


class SqliteRelease(Release):
    # PEEWEE FIELDS

    id = peewee.IntegerField(primary_key=True)
    artists = sqlite_ext.JSONField(null=True, index=False)
    companies = sqlite_ext.JSONField(null=True, index=False)
    country = peewee.TextField(null=True, index=False)
    extra_artists = sqlite_ext.JSONField(null=True, index=False)
    formats = sqlite_ext.JSONField(null=True, index=False)
    genres = sqlite_ext.JSONField(null=True, index=False)
    identifiers = sqlite_ext.JSONField(null=True, index=False)
    labels = sqlite_ext.JSONField(null=True, index=False)
    master_id = peewee.IntegerField(null=True, index=False)
    notes = peewee.TextField(null=True, index=False)
    release_date = peewee.DateTimeField(null=True, index=False)
    styles = sqlite_ext.JSONField(null=True, index=False)
    title = peewee.TextField(index=False)
    tracklist = sqlite_ext.JSONField(null=True, index=False)

    # PEEWEE META

    class Meta:
        table_name = "release"
