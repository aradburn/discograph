import logging

import peewee
from playhouse import postgres_ext

from discograph.library.entity_type import EntityType
from discograph.library.enum_field import EnumField
from discograph.library.models.relation import Relation


log = logging.getLogger(__name__)


class PostgresRelation(Relation):
    # PEEWEE FIELDS

    entity_one_type = EnumField(index=False, choices=EntityType)
    entity_one_id = peewee.IntegerField(index=False)
    entity_two_type = EnumField(index=False, choices=EntityType)
    entity_two_id = peewee.IntegerField(index=False)
    role = peewee.CharField(index=False)
    releases = postgres_ext.BinaryJSONField(index=False, null=True)

    # PEEWEE META

    class Meta:
        table_name = "relation"


# Create an index on entity_one_id
idx_entity_one_id = PostgresRelation.index(
    PostgresRelation.entity_one_id, unique=False, safe=True, using="hash"
)
PostgresRelation.add_index(idx_entity_one_id)

# Create an index on entity_two_id
idx_entity_two_id = PostgresRelation.index(
    PostgresRelation.entity_two_id, unique=False, safe=True, using="hash"
)
PostgresRelation.add_index(idx_entity_two_id)
