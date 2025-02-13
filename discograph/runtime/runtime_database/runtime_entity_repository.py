import logging
from typing import Generator, Any, cast, List

from sqlalchemy import Result, select, update, Select, delete, func

from discograph import utils
from discograph.exceptions import NotFoundError, DatabaseError, UnprocessableError
from discograph.library.fields.entity_type import EntityType
from discograph.runtime.runtime_database import RuntimeEntityTable
from discograph.runtime.runtime_database.runtime_base_repository import (
    RuntimeBaseRepository,
)
from discograph.runtime.runtime_domain.entity import RuntimeEntity

log = logging.getLogger(__name__)


class RuntimeEntityRepository(RuntimeBaseRepository[RuntimeEntityTable]):
    schema_class = RuntimeEntityTable

    @staticmethod
    def _to_domain(entity_db: RuntimeEntity) -> RuntimeEntity:
        # print(f"_to_domain")
        return entity_db

    def _get_one_by_query(
        self, query: Select[tuple[RuntimeEntityTable]]
    ) -> RuntimeEntity:
        # print(f"_get_one_by_query")
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        # print(f"instance: {instance}")

        entity_db = RuntimeEntity.model_validate(instance)
        # print(f"relation_db: {relation_d)}")
        return self._to_domain(entity_db)

    def _get_all_by_query(
        self, query: Select[tuple[RuntimeEntityTable]]
    ) -> List[RuntimeEntity]:
        # print(f"_get_all_by_query")
        result: Result = self.execute(query)

        instances = result.scalars().all()
        entity_dbs = [RuntimeEntity.model_validate(instance) for instance in instances]
        return list(map(self._to_domain, entity_dbs))

    def count_by_type(self, entity_type: EntityType) -> int:
        query = (
            select(func.count())
            .select_from(self.schema_class)
            .where(RuntimeEntityTable.entity_type == entity_type)
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

    def all(self) -> Generator[RuntimeEntity, None, None]:
        query = select(RuntimeEntityTable)
        with self._session.execute(
            query, execution_options={"yield_per": 1000}
        ) as results:
            for partition in results.partitions():
                # partition is an iterable that will be at most 1000 items
                for row in partition:
                    yield RuntimeEntity.model_validate(row[0])

    def all_ids_and_names(self) -> Generator[tuple[int, str], None, None]:
        query = select(RuntimeEntityTable.id, RuntimeEntityTable.entity_name)
        with self._session.execute(
            query, execution_options={"yield_per": 1000}
        ) as results:
            for partition in results.partitions():
                # partition is an iterable that will be at most 1000 items
                for row in partition:
                    yield row

    def get_by_id(self, id_: int) -> RuntimeEntity:
        query = select(RuntimeEntityTable).where(RuntimeEntityTable.id == id_)
        return self._get_one_by_query(query)

    def get_by_entity_id_and_entity_type(
        self, entity_id: int, entity_type: EntityType
    ) -> RuntimeEntity:
        query = select(RuntimeEntityTable).where(
            (RuntimeEntityTable.entity_id == entity_id)
            & (RuntimeEntityTable.entity_type == entity_type)
        )
        return self._get_one_by_query(query)

    def get_ids(self):
        return self._session.scalars(select(RuntimeEntityTable.id)).all()

    def get_ids_by_type(self, entity_type: EntityType):
        return self._session.scalars(
            select(RuntimeEntityTable.id).where(
                RuntimeEntityTable.entity_type == entity_type
            )
        ).all()

    def get_entity_ids_by_type(self, entity_type: EntityType):
        return self._session.scalars(
            select(RuntimeEntityTable.entity_id).where(
                RuntimeEntityTable.entity_type == entity_type
            )
        ).all()

    def get_entity_id_by_entity_type_and_entity_name(
        self, entity_type: EntityType, entity_name: str
    ):
        return self._session.execute(
            select(RuntimeEntityTable.entity_id).where(
                (RuntimeEntityTable.entity_name == entity_name)
                & (RuntimeEntityTable.entity_type == entity_type)
            )
        ).scalar_one_or_none()

    def get_id_by_entity_type_and_entity_name(
        self, entity_type: EntityType, entity_name: str
    ):
        return self._session.execute(
            select(RuntimeEntityTable.id).where(
                (RuntimeEntityTable.entity_name == entity_name)
                & (RuntimeEntityTable.entity_type == entity_type)
            )
        ).scalar_one_or_none()

    def get_id_by_entity_type_and_entity_id(
        self, entity_type: EntityType, entity_id: int
    ):
        return self._session.execute(
            select(RuntimeEntityTable.id).where(
                (RuntimeEntityTable.entity_id == entity_id)
                & (RuntimeEntityTable.entity_type == entity_type)
            )
        ).scalar_one_or_none()

    def get_batched_ids(self, num_in_batch: int):
        return utils.batched(self.get_ids(), num_in_batch)

    # def get_batched_ids_by_type(self, entity_type: EntityType, num_in_batch: int):
    #     return utils.batched(self.get_ids_by_type(entity_type), num_in_batch)

    # def find_by_search_content(self, search_string: str) -> List[RuntimeEntity]:
    #     query = select(RuntimeEntityTable).where(
    #         RuntimeEntityTable.search_content.match(search_string)
    #     )
    #     # log.debug(f"search: {query}")
    #     return self._get_all_by_query(query)

    def create(self, entity: RuntimeEntity) -> RuntimeEntity:
        instance: RuntimeEntityTable = self._save(entity.model_dump())
        # instance: EntityTable = await self._save(schema.model_dump())
        return RuntimeEntity.model_validate(instance)

    def get_by_type_and_name(
        self, entity_type: EntityType, entity_name: str
    ) -> RuntimeEntity:
        query = (
            select(RuntimeEntityTable)
            .where(
                (RuntimeEntityTable.entity_type == entity_type)
                & (RuntimeEntityTable.entity_name == entity_name)
            )
            .limit(1)
        )
        return self._get_one_by_query(query)

    # def get_random_by_id(self, id_: int) -> Entity:
    #     query = (
    #         select(EntityTable)
    #         .where(
    #             (EntityTable.id == id_)
    #             & (EntityTable.entity_type == EntityType.ARTIST)
    #             & (cast(EntityTable.entities, String) != "{}")
    #             & (cast(EntityTable.relation_counts, String) != "{}")
    #             & (
    #                 (EntityTable.relation_counts["Member Of"].cast(String) != "{}")
    #                 | (EntityTable.relation_counts["Alias"].cast(String) != "{}")
    #                 | (EntityTable.entities["members"].cast(String) != "{}")
    #             )
    #         )
    #         .order_by(EntityTable.random)
    #         .limit(1)
    #     )
    #     return self._get_one_by_query(query)

    # def get_random(self) -> Entity:
    #     n = random()
    #     query = (
    #         select(EntityTable)
    #         .where(
    #             (EntityTable.random >= n)
    #             & (EntityTable.entity_type == EntityType.ARTIST)
    #             # # & (cast(EntityTable.entities, String) != "{}")
    #             # # & (cast(EntityTable.relation_counts, String) != "{}")
    #             # & (
    #             #     (EntityTable.relation_counts["Member Of"].cast(String) != "{}")
    #             #     | (EntityTable.relation_counts["Alias"].cast(String) != "{}")
    #             #     | (EntityTable.entities["members"].cast(String) != "{}")
    #             # )
    #         )
    #         .order_by(EntityTable.random)
    #         .limit(1)
    #     )
    #     return self._get_one_by_query(query)

    def update(
        self,
        id_: int,
        payload: dict[str, Any],
    ) -> RuntimeEntityTable:
        """Updates an existed instance of the model in the related table.
        If some data is not exist in the payload then the null value will
        be passed to the schema class."""

        query = (
            update(self.schema_class)
            .where(RuntimeEntityTable.id == id_)
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

    def delete_by_id(self, id_: int) -> None:
        self.execute(delete(self.schema_class).where(RuntimeEntityTable.id == id_))
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        # self._session.flush()
        # await self._session.flush()

    # def search_multi(self, entity_ids: list[int]) -> List[Entity]:
    #     where_clause = cast(
    #         "ColumnElement[bool]", EntityTable.entity_id.in_(entity_ids)
    #     )
    #
    #     # log.debug(f"            search_multi where_clause: {where_clause}")
    #     query = select(EntityTable).where(where_clause)
    #     return self._get_all_by_query(query)

    def search_multi(self, entity_keys) -> List[RuntimeEntity]:
        artist_ids: List[int] = []
        label_ids: List[int] = []
        for entity_id, entity_type in entity_keys:
            if entity_type == EntityType.ARTIST:
                artist_ids.append(entity_id)
            elif entity_type == EntityType.LABEL:
                label_ids.append(entity_id)
        if artist_ids and label_ids:
            where_clause = (
                (RuntimeEntityTable.entity_type == EntityType.ARTIST)
                & cast(
                    "ColumnElement[bool]",
                    (RuntimeEntityTable.entity_id.in_(artist_ids)),
                )
            ) | (
                (RuntimeEntityTable.entity_type == EntityType.LABEL)
                & cast(
                    "ColumnElement[bool]", (RuntimeEntityTable.entity_id.in_(label_ids))
                )
            )
        elif artist_ids:
            where_clause = (RuntimeEntityTable.entity_type == EntityType.ARTIST) & (
                cast(
                    "ColumnElement[bool]", RuntimeEntityTable.entity_id.in_(artist_ids)
                )
            )
        else:
            where_clause = (RuntimeEntityTable.entity_type == EntityType.LABEL) & (
                cast("ColumnElement[bool]", RuntimeEntityTable.entity_id.in_(label_ids))
            )
        # log.debug(f"            search_multi where_clause: {where_clause}")
        query = select(RuntimeEntityTable).where(where_clause)
        return self._get_all_by_query(query)
