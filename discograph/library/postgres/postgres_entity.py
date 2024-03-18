import json
import logging
import random

import peewee
from playhouse import postgres_ext
from playhouse.shortcuts import model_to_dict
from unidecode import unidecode

from discograph.library.entity_type import EntityType
from discograph.library.enum_field import EnumField
from discograph.library.models.entity import Entity
from discograph.library.models.relation import Relation
from discograph.library.postgres.postgres_relation import PostgresRelation


log = logging.getLogger(__name__)


class PostgresEntity(Entity):
    def __format__(self, format_specification=""):
        return json.dumps(
            model_to_dict(
                self,
                exclude=[
                    PostgresEntity.random,
                    PostgresEntity.relation_counts,
                    PostgresEntity.search_content,
                ],
            ),
            indent=4,
            sort_keys=True,
            default=str,
        )

    # PEEWEE FIELDS

    entity_id = peewee.IntegerField(index=False)
    entity_type = EnumField(index=False, choices=EntityType)
    name = peewee.TextField(index=True)
    relation_counts = postgres_ext.BinaryJSONField(null=True, index=False)
    metadata = postgres_ext.BinaryJSONField(null=True, index=False)
    entities = postgres_ext.BinaryJSONField(null=True, index=False)
    search_content = postgres_ext.TSVectorField(index=True)

    # PEEWEE META

    class Meta:
        table_name = "entity"

    @classmethod
    def search_text(cls, search_string):
        search_string = search_string.lower()
        # Transliterate the unicode string into a plain ASCII string
        search_string = unidecode(search_string, "preserve")
        search_string = ",".join(search_string.split())
        query = PostgresEntity.raw(
            """
            SELECT entity_type,
                entity_id,
                name,
                ts_rank_cd(search_content, query, 63) AS rank
            FROM postgresentity,
                to_tsquery(%s) query
            WHERE query @@ search_content
            ORDER BY rank DESC
            LIMIT 100
            """,
            search_string,
        )
        return query

    @classmethod
    def string_to_tsvector(cls, string):
        string = string.lower()
        # Transliterate the unicode string into a plain ASCII string
        string = unidecode(string, "preserve")
        string = cls._strip_pattern.sub("", string)
        tsvector = peewee.fn.to_tsvector(string)
        return tsvector

    @classmethod
    def create_relation(
        cls, entity_one_type, entity_one_id, entity_two_type, entity_two_id, role
    ) -> Relation:
        return PostgresRelation(
            entity_one_type=entity_one_type,
            entity_one_id=entity_one_id,
            entity_two_type=entity_two_type,
            entity_two_id=entity_two_id,
            role=role,
        )

    @classmethod
    def get_random(cls):
        n = random.random()
        return (
            cls.select()
            .where(
                (cls.random > n)
                & (cls.entity_type == EntityType.ARTIST)
                & ~(cls.entities.is_null())
                & ~(cls.relation_counts.is_null())
            )
            .order_by(cls.random)
            .get()
        )
