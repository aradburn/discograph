import logging
import random

import peewee
from playhouse import sqlite_ext

from discograph.library.discogs_model import DiscogsModel
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.enum_field import EnumField
from discograph.library.models.relation import Relation


log = logging.getLogger(__name__)


class SqliteRelation(Relation):
    # PEEWEE FIELDS

    entity_one_type = EnumField(index=False, choices=EntityType)
    entity_one_id = peewee.IntegerField(index=True)
    entity_two_type = EnumField(index=False, choices=EntityType)
    entity_two_id = peewee.IntegerField(index=True)
    role = peewee.CharField(index=False)
    releases = sqlite_ext.JSONField(null=True, index=False)
    random = peewee.FloatField(index=True, null=True)

    # PEEWEE META

    class Meta:
        table_name = "relation"

    @classmethod
    def create_or_update_relation(cls, relation):
        with DiscogsModel.atomic():
            # log.debug("loader_pass_one_inner relation update")
            try:
                instance = (
                    cls.select().where(
                        (cls.entity_one_type == relation["entity_one_type"])
                        & (cls.entity_one_id == relation["entity_one_id"])
                        & (cls.entity_two_type == relation["entity_two_type"])
                        & (cls.entity_two_id == relation["entity_two_id"])
                        & (cls.role == relation["role"])
                    )
                    # .for_update(nowait=True)
                    .get()
                )
            except peewee.DoesNotExist:
                # Create a new Relation
                cls.create(
                    entity_one_type=relation["entity_one_type"],
                    entity_one_id=relation["entity_one_id"],
                    entity_two_type=relation["entity_two_type"],
                    entity_two_id=relation["entity_two_id"],
                    role=relation["role"],
                    releases={},
                    random=random.random(),
                )
                instance = (
                    cls.select().where(
                        (cls.entity_one_type == relation["entity_one_type"])
                        & (cls.entity_one_id == relation["entity_one_id"])
                        & (cls.entity_two_type == relation["entity_two_type"])
                        & (cls.entity_two_id == relation["entity_two_id"])
                        & (cls.role == relation["role"])
                    )
                    # .for_update(nowait=True)
                    .get()
                )

            if "release_id" in relation:
                release_id = relation["release_id"]
                year = relation.get("year")
                if not instance.releases:
                    instance.releases = {}
                instance.releases[release_id] = year
            instance.save()
