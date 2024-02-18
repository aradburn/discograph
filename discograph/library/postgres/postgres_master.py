import logging

import peewee
from playhouse import postgres_ext

from discograph.library.bootstrapper import Bootstrapper
from discograph.library.discogs_model import DiscogsModel
from discograph.library.postgres.postgres_release import PostgresRelease


log = logging.getLogger(__name__)


class PostgresMaster(DiscogsModel):
    # PEEWEE FIELDS

    id = peewee.IntegerField(primary_key=True)
    artists = postgres_ext.BinaryJSONField(null=True)
    genres = postgres_ext.ArrayField(peewee.TextField, null=True)
    main_release_id = peewee.IntegerField(null=True)
    styles = postgres_ext.ArrayField(peewee.TextField, null=True)
    title = peewee.TextField()
    year = peewee.IntegerField(null=True)

    # PEEWEE META

    class Meta:
        table_name = "masters"

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
            xml_tag="master",
            name_attr="title",
            skip_without=["title"],
        )


PostgresMaster._tags_to_fields_mapping = {
    "artists": ("artists", PostgresRelease.element_to_artist_credits),
    "genres": ("genres", Bootstrapper.element_to_strings),
    "main_release": ("main_release_id", Bootstrapper.element_to_integer),
    "styles": ("styles", Bootstrapper.element_to_strings),
    "title": ("title", Bootstrapper.element_to_string),
    "year": ("year", Bootstrapper.element_to_integer),
}
