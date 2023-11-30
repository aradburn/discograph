import json

import peewee
from playhouse import postgres_ext
from playhouse.cockroachdb import JSONField
from playhouse.shortcuts import model_to_dict
from unidecode import unidecode

from discograph.library import EntityType
from discograph.library.EnumField import EnumField
from discograph.library.models.entity import Entity
from discograph.library.cockroach.cockroach_relation import CockroachRelation


class CockroachEntity(Entity):

    def __format__(self, format_specification=''):
        return json.dumps(
            model_to_dict(self, exclude=[
                CockroachEntity.random,
                CockroachEntity.relation_counts,
                CockroachEntity.search_content,
            ]),
            indent=4,
            sort_keys=True,
            default=str
        )

    # PEEWEE FIELDS

    entity_id = peewee.IntegerField(index=False)
    entity_type = EnumField(index=False, choices=EntityType)
    name = peewee.TextField(index=True)
    relation_counts = JSONField(index=False, null=True)
    metadata = JSONField(index=False, null=True)
    entities = JSONField(index=False, null=True)
    search_content = postgres_ext.TSVectorField(index=True)

    @classmethod
    def search_text(cls, search_string):
        search_string = search_string.lower()
        # Transliterate the unicode string into a plain ASCII string
        search_string = unidecode(search_string, "preserve")
        search_string = ','.join(search_string.split())
        query = CockroachEntity.raw("""
            SELECT entity_type,
                entity_id,
                name,
                ts_rank_cd(search_content, query, 63) AS rank
            FROM entities,
                to_tsquery(%s) query
            WHERE query @@ search_content
            ORDER BY rank DESC
            LIMIT 100
            """, search_string)
        return query

    @classmethod
    def string_to_tsvector(cls, string):
        string = string.lower()
        # Transliterate the unicode string into a plain ASCII string
        string = unidecode(string, "preserve")
        string = cls._strip_pattern.sub('', string)
        tsvector = peewee.fn.to_tsvector(string)
        return tsvector

    @classmethod
    def create_relation(cls, entity_one_type, entity_one_id, entity_two_type, entity_two_id, role):
        return CockroachRelation(
                        entity_one_type=entity_one_type,
                        entity_one_id=entity_one_id,
                        entity_two_type=entity_two_type,
                        entity_two_id=entity_two_id,
                        role=role,
                        )
