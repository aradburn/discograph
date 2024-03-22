import peewee
from playhouse.cockroachdb import JSONField, ArrayField

from discograph.library.cockroach.cockroach_release import CockroachRelease
from discograph.library.discogs_model import DiscogsModel
from discograph.library.loader_utils import LoaderUtils


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

    # @classmethod
    # def bootstrap(cls):
    #     cls.drop_table(True)
    #     cls.create_table()
    #     cls.bootstrap_pass_one()

    @classmethod
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        # noinspection PyArgumentList
        return cls(**data)

    @classmethod
    def loader_pass_one(cls, date: str):
        DiscogsModel.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="master",
            id_attr="id",
            name_attr="title",
            skip_without=["title"],
        )


CockroachMaster._tags_to_fields_mapping = {
    "artists": ("artists", CockroachRelease.element_to_artist_credits),
    "genres": ("genres", LoaderUtils.element_to_strings),
    "main_release": ("main_release_id", LoaderUtils.element_to_integer),
    "styles": ("styles", LoaderUtils.element_to_strings),
    "title": ("title", LoaderUtils.element_to_string),
    "year": ("year", LoaderUtils.element_to_integer),
}
