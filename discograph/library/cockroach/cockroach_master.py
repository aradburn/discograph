import peewee
from playhouse.cockroachdb import JSONField, ArrayField

from discograph.library.bootstrapper import Bootstrapper
from discograph.library.cockroach.cockroach_release import CockroachRelease
from discograph.library.discogs_model import DiscogsModel


class CockroachMaster(DiscogsModel):
    # PEEWEE FIELDS

    id = peewee.IntegerField(primary_key=True)
    artists = JSONField(null=True)
    genres = ArrayField(peewee.TextField, null=True)
    main_release_id = peewee.IntegerField(null=True)
    styles = ArrayField(peewee.TextField, null=True)
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


CockroachMaster._tags_to_fields_mapping = {
    "artists": ("artists", CockroachRelease.element_to_artist_credits),
    "genres": ("genres", Bootstrapper.element_to_strings),
    "main_release": ("main_release_id", Bootstrapper.element_to_integer),
    "styles": ("styles", Bootstrapper.element_to_strings),
    "title": ("title", Bootstrapper.element_to_string),
    "year": ("year", Bootstrapper.element_to_integer),
}
