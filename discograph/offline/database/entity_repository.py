import logging
from typing import Generator, Any, cast, List, Sequence

from sqlalchemy import Result, select, update, Select, delete, func

from discograph import utils
from discograph.exceptions import NotFoundError, DatabaseError, UnprocessableError
from discograph.offline.database.base_repository import BaseRepository
from discograph.offline.database.entity_table import EntityTable
from discograph.offline.domain.entity import Entity
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class EntityRepository(BaseRepository[EntityTable]):
    schema_class = EntityTable

    def _get_one_by_query(self, query: Select[tuple[EntityTable]]) -> Entity:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        entity_db = Entity.model_validate(instance)
        return entity_db.to_domain()

    def _get_all_by_query(self, query: Select[tuple[EntityTable]]) -> List[Entity]:
        result: Result = self.execute(query)

        instances = result.scalars().all()
        entity_dbs = [Entity.model_validate(instance) for instance in instances]
        entities = [entity_db.to_domain() for entity_db in entity_dbs]
        return entities

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
        query = select(EntityTable)
        with self._session.execute(
            query, execution_options={"yield_per": 1000}
        ) as results:
            for partition in results.partitions():
                # partition is an iterable that will be at most 1000 items
                for row in partition:
                    yield Entity.model_validate(row[0])

    def all_ids_and_names(self) -> Generator[tuple[int, str], None, None]:
        query = select(EntityTable.id, EntityTable.entity_name)
        with self._session.execute(
            query, execution_options={"yield_per": 1000}
        ) as results:
            for partition in results.partitions():
                # partition is an iterable that will be at most 1000 items
                for row in partition:
                    yield row

    def get_by_id(self, id_: int) -> Entity:
        query = select(EntityTable).where(EntityTable.id == id_)
        return self._get_one_by_query(query)

    def get_by_entity_id_and_entity_type(
        self, entity_id: int, entity_type: EntityType
    ) -> Entity:
        query = select(EntityTable).where(
            (EntityTable.entity_id == entity_id)
            & (EntityTable.entity_type == entity_type)
        )
        return self._get_one_by_query(query)

    def get_ids(self) -> Sequence[int]:
        return self._session.scalars(select(EntityTable.id)).all()

    def get_ids_by_type(self, entity_type: EntityType) -> Sequence[int]:
        return self._session.scalars(
            select(EntityTable.id).where(EntityTable.entity_type == entity_type)
        ).all()

    def get_entity_ids_by_type(self, entity_type: EntityType) -> Sequence[int]:
        return self._session.scalars(
            select(EntityTable.entity_id).where(EntityTable.entity_type == entity_type)
        ).all()

    def get_entity_id_by_entity_type_and_entity_name(
        self, entity_type: EntityType, entity_name: str
    ) -> int | None:
        return self._session.execute(
            select(EntityTable.entity_id).where(
                (EntityTable.entity_name == entity_name)
                & (EntityTable.entity_type == entity_type)
            )
        ).scalar_one_or_none()

    def get_id_by_entity_type_and_entity_name(
        self, entity_type: EntityType, entity_name: str
    ) -> int | None:
        return self._session.execute(
            select(EntityTable.id).where(
                (EntityTable.entity_name == entity_name)
                & (EntityTable.entity_type == entity_type)
            )
        ).scalar_one_or_none()

    def get_id_by_entity_type_and_entity_id(
        self, entity_type: EntityType, entity_id: int
    ) -> int | None:
        return self._session.execute(
            select(EntityTable.id).where(
                (EntityTable.entity_id == entity_id)
                & (EntityTable.entity_type == entity_type)
            )
        ).scalar_one_or_none()

    def get_batched_ids(self, num_in_batch: int) -> List[List[int]]:
        return utils.batched(self.get_ids(), num_in_batch)

    def find_by_search_content(self, search_string: str) -> List[Entity]:
        query = select(EntityTable).where(
            EntityTable.search_content.match(search_string)
        )
        return self._get_all_by_query(query)

    def create(self, entity: Entity) -> Entity:
        instance: EntityTable = self._save(entity.model_dump())
        # instance: EntityTable = await self._save(schema.model_dump())
        return Entity.model_validate(instance)

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

    def update(
        self,
        id_: int,
        payload: dict[str, Any],
    ) -> Entity:
        """Updates an existed instance of the model in the related table.
        If some data is not exist in the payload then the null value will
        be passed to the schema class."""

        query = (
            update(self.schema_class)
            .where(EntityTable.id == id_)
            .values(payload)
            .returning(self.schema_class)
        )
        result: Result = self._session.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (instance := result.scalar_one_or_none()):
            raise DatabaseError

        entity_db = Entity.model_validate(instance)
        return entity_db.to_domain()

    def delete_by_id(self, id_: int) -> None:
        self.execute(delete(self.schema_class).where(EntityTable.id == id_))
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
        query = select(EntityTable).where(where_clause)
        return self._get_all_by_query(query)
