import logging
import re

from sqlalchemy import ColumnElement
from unidecode import unidecode

from discograph.library.fields.entity_type import EntityType
from discograph.library.models.entity import Entity
from discograph.library.models.relation import Relation
from discograph.library.postgres.postgres_relation import PostgresRelation

log = logging.getLogger(__name__)


class PostgresEntity(Entity):
    # COLUMNS
    # search_content = postgres_ext.TSVectorField(index=True)

    # entity_id = peewee.IntegerField(index=False)
    # entity_type = EnumField(index=False, choices=EntityType)
    # name = peewee.TextField(index=True)
    # relation_counts = postgres_ext.BinaryJSONField(null=True, index=False)
    # metadata = postgres_ext.BinaryJSONField(null=True, index=False)
    # entities = postgres_ext.BinaryJSONField(null=True, index=False)
    # search_content = postgres_ext.TSVectorField(index=True)
    # random = peewee.FloatField(index=True, null=True)

    @classmethod
    def build_search_text_query(cls, search_string) -> ColumnElement[bool]:
        return cls.search_content.match(search_string)
        # search_string = search_string.lower()
        # # Transliterate the unicode string into a plain ASCII string
        # search_string = unidecode(search_string, "preserve")
        # search_string = ",".join(search_string.split())
        # # TODO fix search_string injection
        # query = f"""
        #     SELECT entity_type,
        #         entity_id,
        #         name,
        #         ts_rank_cd(search_content, query, 63) AS rank
        #     FROM postgresentity,
        #         to_tsquery({search_string}) query
        #     WHERE query @@ search_content
        #     ORDER BY rank DESC
        #     LIMIT 100
        #     """
        # return query

    # @classmethod
    # def get_search_content(cls, string):
    #     string = string.lower()
    #     # Transliterate the unicode string into a plain ASCII string
    #     string = unidecode(string, "preserve")
    #     string = cls._strip_pattern.sub("", string)
    #     return string
    #     # tsvector = func.to_tsvector("english", string)
    #     # return tsvector

    @classmethod
    def create_relation(
        cls,
        entity_one_id: int,
        entity_one_type: EntityType,
        entity_two_id: int,
        entity_two_type: EntityType,
        role: str,
    ) -> Relation:
        return PostgresRelation(
            entity_one_id=entity_one_id,
            entity_one_type=entity_one_type,
            entity_two_id=entity_two_id,
            entity_two_type=entity_two_type,
            role=role,
        )
