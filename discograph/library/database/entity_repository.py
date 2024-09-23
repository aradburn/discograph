import logging
from random import random
from typing import Generator, Any, cast, List

from sqlalchemy import Result, select, update, Select, String, delete, func

from discograph import utils
from discograph.exceptions import NotFoundError, DatabaseError, UnprocessableError
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.entity_table import EntityTable
from discograph.library.domain.entity import Entity
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class EntityRepository(BaseRepository[EntityTable]):
    schema_class = EntityTable

    @staticmethod
    def _to_domain(entity_db: Entity) -> Entity:
        # print(f"_to_domain")
        return entity_db

    def _get_one_by_query(self, query: Select[tuple[EntityTable]]) -> Entity:
        # print(f"_get_one_by_query")
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        # print(f"instance: {instance}")

        entity_db = Entity.model_validate(instance)
        # print(f"relation_db: {relation_d)}")
        return self._to_domain(entity_db)

    def _get_all_by_query(self, query: Select[tuple[EntityTable]]) -> List[Entity]:
        # print(f"_get_all_by_query")
        result: Result = self.execute(query)

        instances = result.scalars().all()
        entity_dbs = [Entity.model_validate(instance) for instance in instances]
        return list(map(self._to_domain, entity_dbs))

    def count_by_type(self, entity_type: EntityType) -> int:
        query = (
            select(func.count())
            .select_from(self.schema_class)
            .where(EntityTable.entity_type == entity_type)
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(func.count(self.schema_class.id))
        value = result.scalar()

        if not isinstance(value, int):
            raise UnprocessableError(
                message=(
                    "For some reason count function returned not an integer."
                    f"Value: {value}"
                ),
            )

        return value

    def all(self) -> Generator[Entity, None, None]:
        for instance in self._all():
            yield Entity.model_validate(instance)

    def get(self, entity_id: int, entity_type: EntityType) -> Entity:
        query = select(EntityTable).where(
            (EntityTable.entity_id == entity_id)
            & (EntityTable.entity_type == entity_type)
        )
        return self._get_one_by_query(query)

    def get_ids(self, entity_type: EntityType):
        return self._session.scalars(
            select(EntityTable.entity_id).where(EntityTable.entity_type == entity_type)
            # select(ReleaseTable.release_id).order_by(ReleaseTable.release_id)
        ).all()

    def get_batched_ids(self, entity_type: EntityType, num_in_batch: int):
        return utils.batched(self.get_ids(entity_type), num_in_batch)

    def find_by_search_content(self, search_string: str) -> List[Entity]:
        # print(f"find_by_search_content")
        query = select(EntityTable).where(
            EntityTable.search_content.match(search_string)
        )
        return self._get_all_by_query(query)

    def create(self, entity: Entity) -> Entity:
        instance: EntityTable = self._save(entity.model_dump())
        # instance: EntityTable = await self._save(schema.model_dump())
        return Entity.model_validate(instance)

    # use all() def get_entity_iterator(self, entity_type: EntityType):
    #     entity_ids = self._session.scalars(
    #         select(EntityTable.entity_id).where(EntityTable.entity_type == entity_type)
    #     ).all()
    #     for entity_id in entity_ids:
    #         entity = self.get(entity_id, entity_type)
    #         yield entity

    def get_chunked_entity_ids(self, entity_type: EntityType):
        from discograph.database import get_concurrency_count

        all_ids = self._session.scalars(
            select(EntityTable.entity_id)
            .where(EntityTable.entity_type == entity_type)
            .order_by(EntityTable.entity_id)
        ).all()

        num_chunks = get_concurrency_count()
        return utils.split_tuple(num_chunks, all_ids)

    def get_by_type_and_name(self, entity_type: EntityType, entity_name: str) -> Entity:
        query = (
            select(EntityTable)
            .where(
                (EntityTable.entity_type == entity_type)
                & (EntityTable.entity_name == entity_name)
            )
            .limit(1)
        )
        return self._get_one_by_query(query)

    def get_random(self) -> Entity:
        n = random()
        query = (
            select(EntityTable)
            .where(
                (EntityTable.random > n)
                & (EntityTable.entity_type == EntityType.ARTIST)
                # & (cast(EntityTable.entities, String) != "{}")
                # & (cast(EntityTable.relation_counts, String) != "{}")
                & (
                    (EntityTable.relation_counts["Member Of"].cast(String) != "{}")
                    | (EntityTable.relation_counts["Alias"].cast(String) != "{}")
                    | (EntityTable.entities["members"].cast(String) != "{}")
                )
            )
            .order_by(EntityTable.random)
            .limit(1)
        )
        return self._get_one_by_query(query)

    def update(
        self,
        entity_id: int,
        entity_type: EntityType,
        payload: dict[str, Any],
    ) -> EntityTable:
        """Updates an existed instance of the model in the related table.
        If some data is not exist in the payload then the null value will
        be passed to the schema class."""

        query = (
            update(self.schema_class)
            .where(
                (EntityTable.entity_id == entity_id)
                & (EntityTable.entity_type == entity_type)
            )
            .values(payload)
            .returning(self.schema_class)
        )
        result: Result = self._session.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (schema := result.scalar_one_or_none()):
            raise DatabaseError

        return schema

    def delete_by_id(self, entity_id: int, entity_type: EntityType) -> None:
        self.execute(
            delete(self.schema_class).where(
                (EntityTable.entity_id == entity_id)
                & (EntityTable.entity_type == entity_type)
            )
        )
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        # self._session.flush()
        # await self._session.flush()

    def search_multi(self, entity_keys) -> List[Entity]:
        artist_ids: List[int] = []
        label_ids: List[int] = []
        for entity_id, entity_type in entity_keys:
            if entity_type == EntityType.ARTIST:
                artist_ids.append(entity_id)
            elif entity_type == EntityType.LABEL:
                label_ids.append(entity_id)
        if artist_ids and label_ids:
            where_clause = (
                (EntityTable.entity_type == EntityType.ARTIST)
                & cast("ColumnElement[bool]", (EntityTable.entity_id.in_(artist_ids)))
            ) | (
                (EntityTable.entity_type == EntityType.LABEL)
                & cast("ColumnElement[bool]", (EntityTable.entity_id.in_(label_ids)))
            )
        elif artist_ids:
            where_clause = (EntityTable.entity_type == EntityType.ARTIST) & (
                cast("ColumnElement[bool]", EntityTable.entity_id.in_(artist_ids))
            )
        else:
            where_clause = (EntityTable.entity_type == EntityType.LABEL) & (
                cast("ColumnElement[bool]", EntityTable.entity_id.in_(label_ids))
            )
        # log.debug(f"            search_multi where_clause: {where_clause}")
        query = select(EntityTable).where(where_clause)
        return self._get_all_by_query(query)
