import peewee
from playhouse.cockroachdb import JSONField

from discograph.library import EntityType
from discograph.library.enum_field import EnumField
from discograph.library.models.relation import Relation


class CockroachRelation(Relation):
    # PEEWEE FIELDS

    entity_one_type = EnumField(index=False, choices=EntityType)
    entity_one_id = peewee.IntegerField(index=False)
    entity_two_type = EnumField(index=False, choices=EntityType)
    entity_two_id = peewee.IntegerField(index=False)
    role = peewee.CharField(index=False)
    releases = JSONField(index=False, null=True)
