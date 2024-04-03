from __future__ import annotations

import logging
import re

from sqlalchemy import ColumnElement
from unidecode import unidecode

from discograph.library.fields.entity_type import EntityType
from discograph.library.models.entity import Entity
from discograph.library.models.relation import Relation
from discograph.library.sqlite.sqlite_relation import SqliteRelation

log = logging.getLogger(__name__)


class SqliteEntity(Entity):
    # OVERRIDE COLUMNS

    # entity_id = peewee.IntegerField(index=False)
    # entity_type = EnumField(index=False, choices=EntityType)
    # name = peewee.TextField(index=True)
    # relation_counts = sqlite_ext.JSONField(null=True, index=False)
    # metadata = sqlite_ext.JSONField(null=True, index=False)
    # entities = sqlite_ext.JSONField(null=True, index=False)
    # search_content = sqlite_ext.SearchField()
    # random = peewee.FloatField(index=True, null=True)

    @classmethod
    def build_search_text_query(cls, search_string) -> ColumnElement[bool]:
        return cls.search_content.match(search_string)

    # @classmethod
    # def build_search_text_query(cls, search_string) -> str:
    #     search_string = search_string.lower()
    #     # Transliterate the unicode string into a plain ASCII string
    #     search_string = unidecode(search_string, "preserve")
    #     search_string = ",".join(search_string.split())
    #     log.error(f"Search not impmemented: {search_string}")
    #     query = ""
    #     # query = []
    #     # TODO FTS5 search table
    #     # query = (
    #     #     SqliteEntity.select(SqliteEntity, SqliteEntity.bm25().alias("score"))
    #     #     .where(SqliteEntity.search_content.match(search_string))
    #     #     .order_by(SqliteEntity.bm25())
    #     # )
    #     return query

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
        return SqliteRelation(
            entity_one_id=entity_one_id,
            entity_one_type=entity_one_type,
            entity_two_id=entity_two_id,
            entity_two_type=entity_two_type,
            role=role,
        )
