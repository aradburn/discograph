import re

from unidecode import unidecode

from discograph.library.cockroach.cockroach_relation import CockroachRelation
from discograph.library.fields.entity_type import EntityType
from discograph.library.models.entity import Entity
from discograph.library.models.relation import Relation


class CockroachEntity(Entity):
    # PEEWEE FIELDS

    # entity_id = peewee.IntegerField(index=False)
    # entity_type = EnumField(index=False, choices=EntityType)
    # name = peewee.TextField(index=True)
    # relation_counts = JSONField(index=False, null=True)
    # metadata = JSONField(index=False, null=True)
    # entities = JSONField(index=False, null=True)
    # search_content = postgres_ext.TSVectorField(index=True)
    # random = peewee.FloatField(index=True, null=True)

    @classmethod
    def build_search_text_query(cls, search_string):
        query = "TODO"
        # search_string = search_string.lower()
        # # Transliterate the unicode string into a plain ASCII string
        # search_string = unidecode(search_string, "preserve")
        # search_string = ",".join(search_string.split())
        # query = CockroachEntity.raw(
        #     """
        #     SELECT entity_type,
        #         entity_id,
        #         name,
        #         ts_rank_cd(search_content, query, 63) AS rank
        #     FROM cockroachentity,
        #         to_tsquery(%s) query
        #     WHERE query @@ search_content
        #     ORDER BY rank DESC
        #     LIMIT 100
        #     """,
        #     search_string,
        # )
        return query

    # @classmethod
    # def get_search_content(cls, string):
    #     string = string.lower()
    #     # Transliterate the unicode string into a plain ASCII string
    #     string = unidecode(string, "preserve")
    #     search_content = cls._strip_pattern.sub("", string)
    #     return search_content

    @classmethod
    def create_relation(
        cls,
        entity_one_id: int,
        entity_one_type: EntityType,
        entity_two_id: int,
        entity_two_type: EntityType,
        role: str,
    ) -> Relation:
        return CockroachRelation(
            entity_one_id=entity_one_id,
            entity_one_type=entity_one_type,
            entity_two_id=entity_two_id,
            entity_two_type=entity_two_type,
            role=role,
        )
