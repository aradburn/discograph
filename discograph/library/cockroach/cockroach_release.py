import peewee
from playhouse.cockroachdb import JSONField, ArrayField

from discograph.library.models.release import Release


class CockroachRelease(Release):
    # PEEWEE FIELDS

    id = peewee.IntegerField(primary_key=True)
    artists = JSONField(null=True, index=False)
    companies = JSONField(null=True, index=False)
    country = peewee.TextField(null=True, index=False)
    extra_artists = JSONField(null=True, index=False)
    formats = JSONField(null=True, index=False)
    genres = ArrayField(peewee.TextField, null=True, index=False)
    identifiers = JSONField(null=True, index=False)
    labels = JSONField(null=True, index=False)
    master_id = peewee.IntegerField(null=True, index=False)
    notes = peewee.TextField(null=True, index=False)
    release_date = peewee.DateTimeField(null=True, index=False)
    styles = ArrayField(peewee.TextField, null=True, index=False)
    title = peewee.TextField(index=False)
    tracklist = JSONField(null=True, index=False)
