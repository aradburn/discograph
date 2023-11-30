import peewee
from playhouse import sqlite_ext

from discograph.library import EntityType
from discograph.library.EnumField import EnumField
from discograph.library.models.relation import Relation


class SqliteRelation(Relation):

    # PEEWEE FIELDS

    entity_one_type = EnumField(index=False, choices=EntityType)
    entity_one_id = peewee.IntegerField(index=False)
    entity_two_type = EnumField(index=False, choices=EntityType)
    entity_two_id = peewee.IntegerField(index=False)
    role = peewee.CharField(index=False)
    releases = sqlite_ext.JSONField(null=True, index=False)
