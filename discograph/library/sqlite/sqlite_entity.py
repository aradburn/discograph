from __future__ import annotations

import json
import logging

import peewee
from playhouse import sqlite_ext
from playhouse.shortcuts import model_to_dict
from unidecode import unidecode

from discograph.library.entity_type import EntityType
from discograph.library.enum_field import EnumField
from discograph.library.models.entity import Entity
from discograph.library.models.relation import Relation
from discograph.library.sqlite.sqlite_relation import SqliteRelation
from typing import List, Any


log = logging.getLogger(__name__)


class SqliteEntity(Entity):
    def __format__(self, format_specification="") -> Any:
        return json.dumps(
            model_to_dict(
                self,
                exclude=[
                    SqliteEntity.random,
                    SqliteEntity.relation_counts,
                    SqliteEntity.search_content,
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
    relation_counts = sqlite_ext.JSONField(null=True, index=False)
    metadata = sqlite_ext.JSONField(null=True, index=False)
    entities = sqlite_ext.JSONField(null=True, index=False)
    search_content = sqlite_ext.SearchField()

    @classmethod
    def search_text(cls, search_string) -> List[SqliteEntity]:
        search_string = search_string.lower()
        # Transliterate the unicode string into a plain ASCII string
        search_string = unidecode(search_string, "preserve")
        search_string = ",".join(search_string.split())
        log.error(f"Search not impmemented: {search_string}")
        query = []
        # TODO FTS5 search table
        # query = (
        #     SqliteEntity.select(SqliteEntity, SqliteEntity.bm25().alias("score"))
        #     .where(SqliteEntity.search_content.match(search_string))
        #     .order_by(SqliteEntity.bm25())
        # )
        return query

    # @classmethod
    # def string_to_tsvector(cls, string):
    #     string = string.lower()
    #     # Transliterate the unicode string into a plain ASCII string
    #     string = unidecode(string, "preserve")
    #     string = cls._strip_pattern.sub("", string)
    #     tsvector = string
    #     return tsvector

    @classmethod
    def create_relation(
        cls, entity_one_type, entity_one_id, entity_two_type, entity_two_id, role
    ) -> Relation:
        return SqliteRelation(
            entity_one_type=entity_one_type,
            entity_one_id=entity_one_id,
            entity_two_type=entity_two_type,
            entity_two_id=entity_two_id,
            role=role,
        )
