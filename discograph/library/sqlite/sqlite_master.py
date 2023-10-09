import peewee
from playhouse import sqlite_ext
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.discogs_model import DiscogsModel
from discograph.library.sqlite.sqlite_release import SqliteRelease


class SqliteMaster(DiscogsModel):

    # PEEWEE FIELDS

    id = peewee.IntegerField(primary_key=True)
    artists = sqlite_ext.JSONField(null=True)
    genres = sqlite_ext.JSONField(peewee.TextField, null=True)
    main_release_id = peewee.IntegerField(null=True)
    styles = sqlite_ext.JSONField(peewee.TextField, null=True)
    title = peewee.TextField()
    year = peewee.IntegerField(null=True)

    # PEEWEE META

    class Meta:
        db_table = 'masters'

    # PUBLIC METHODS

    @classmethod
    def bootstrap(cls):
        cls.drop_table(True)
        cls.create_table()
        cls.bootstrap_pass_one()

    @classmethod
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        # noinspection PyArgumentList
        return cls(**data)

    @classmethod
    def bootstrap_pass_one(cls, **kwargs):
        DiscogsModel.bootstrap_pass_one(
            model_class=cls,
            xml_tag='master',
            name_attr='title',
            skip_without=['title'],
        )


SqliteMaster._tags_to_fields_mapping = {
    'artists': ('artists', SqliteRelease.element_to_artist_credits),
    'genres': ('genres', Bootstrapper.element_to_strings),
    'main_release': ('main_release_id', Bootstrapper.element_to_integer),
    'styles': ('styles', Bootstrapper.element_to_strings),
    'title': ('title', Bootstrapper.element_to_string),
    'year': ('year', Bootstrapper.element_to_integer),
}
