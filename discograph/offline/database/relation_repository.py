import logging
from typing import Generator, List

from sqlalchemy import Result, select, Select, delete

from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.cache.role_cache import RoleCache
from discograph.offline.database.base_repository import BaseRepository
from discograph.offline.database.relation_table import RelationTable
from discograph.offline.domain.relation import (
    RelationUncommitted,
    RelationDB,
    RelationInternal,
)

log = logging.getLogger(__name__)


class RelationRepository(BaseRepository[RelationTable]):
    schema_class = RelationTable

    def _get_one_by_query(
        self, query: Select[tuple[RelationTable]]
    ) -> RelationInternal:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        relation_db = RelationDB.model_validate(instance)
        return relation_db.to_domain()

    def _get_all_by_query(
        self, query: Select[tuple[RelationTable]]
    ) -> List[RelationInternal]:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        instances = result.scalars().all()
        relation_dbs = [RelationDB.model_validate(instance) for instance in instances]
        relations = [relation_db.to_domain() for relation_db in relation_dbs]
        return relations

    def all(self) -> Generator[RelationDB, None, None]:
        query = select(RelationTable)
        with self._session.execute(
            query, execution_options={"yield_per": 1000}
        ) as results:
            for partition in results.partitions():
                # partition is an iterable that will be at most 1000 items
                for row in partition:
                    yield RelationDB.model_validate(row[0])

    def get(self, relation_id: int) -> RelationDB:
        # print(f"get")
        query = select(RelationTable).where(RelationTable.id == relation_id)
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)
        # print(f"result: {result}")

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError
        return RelationDB.model_validate(instance)

    def get_id_by_key(self, key: dict) -> int:
        query = select(RelationTable.id).where(
            (RelationTable.subject == key["subject"])
            & (RelationTable.predicate == key["role_id"])
            & (RelationTable.object == key["object"])
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalar()):
            raise NotFoundError
        return instance

    def find_by_id(self, relation_id: int) -> RelationInternal:
        query = (
            select(RelationTable)
            .with_for_update(of=RelationTable, nowait=True)
            .where(RelationTable.id == relation_id)
        )
        return self._get_one_by_query(query)

    def find_by_key(self, key: dict) -> RelationInternal:
        if "role_id" not in key:
            if "role_name" in key:
                role_name = key["role_name"]
                key["role_id"] = RoleCache.role_name_to_role_id_lookup[role_name]
            elif "role" in key:
                role_name = key["role"]
                key["role_id"] = RoleCache.role_name_to_role_id_lookup[role_name]
        query = select(RelationTable).where(
            (RelationTable.subject == key["subject"])
            & (RelationTable.predicate == key["role_id"])
            & (RelationTable.object == key["object"])
        )
        return self._get_one_by_query(query)

    def find_by_entity(self, id_: int) -> List[RelationInternal]:
        # if roles:
        #     where_clause &= RelationTable.role.in_(roles)
        # TODO search by year
        # if year is not None:
        #     year_clause = cls.year.is_null(True)
        #     if isinstance(year, int):
        #         year_clause |= cls.year == year
        #     else:
        #         year_clause |= cls.year.between(year[0], year[1])
        #     where_clause &= year_clause
        query = (
            select(RelationTable)
            .where((RelationTable.subject == id_) | (RelationTable.object == id_))
            .order_by(
                RelationTable.predicate,
                RelationTable.subject,
                RelationTable.object,
            )
        )
        return self._get_all_by_query(query)

    def find_by_entity_and_roles(
        self, id_: int, role_ids: list[int]
    ) -> List[RelationInternal]:
        if id_ is None:
            return []

        # if roles:
        #     where_clause &= RelationTable.role.in_(roles)
        # TODO search by year
        # if year is not None:
        #     year_clause = cls.year.is_null(True)
        #     if isinstance(year, int):
        #         year_clause |= cls.year == year
        #     else:
        #         year_clause |= cls.year.between(year[0], year[1])
        #     where_clause &= year_clause
        query = (
            select(RelationTable)
            .where(
                ((RelationTable.subject == id_) | (RelationTable.object == id_))
                & (RelationTable.predicate.in_(role_ids))
            )
            .order_by(
                RelationTable.predicate,
                RelationTable.subject,
                RelationTable.object,
            )
        )
        return self._get_all_by_query(query)

    def create(
        self, relation: RelationUncommitted, on_conflict_do_nothing=False
    ) -> RelationInternal:
        from discograph.offline.offline_database_manager import OfflineDatabaseManager

        relation_dict = relation.model_dump(exclude={"role_name"})
        role_id = RoleCache.role_name_to_role_id_lookup[relation.role_name]
        relation_dict.update(predicate=role_id)
        query = OfflineDatabaseManager.offline_database_helper.generate_insert_query(
            self.schema_class, relation_dict, on_conflict_do_nothing
        )
        result: Result = self._session.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (instance := result.scalar_one_or_none()):
            raise DatabaseError

        relation_db = RelationDB.model_validate(instance)
        # print(f"relation_db: {utils.normalize_dict(relation_db)}")
        return relation_db.to_domain()

    def create_bulk(
        self, relations: List[RelationUncommitted], on_conflict_do_nothing=False
    ) -> None:
        from discograph.offline.offline_database_manager import OfflineDatabaseManager

        relation_dicts = []
        for relation in relations:
            relation_dict = relation.model_dump(exclude={"role_name"})
            role_id = RoleCache.role_name_to_role_id_lookup[relation.role_name]
            relation_dict.update(predicate=role_id)
            # print(f"relation_dict: {relation_dict}")
            relation_dicts.append(relation_dict)
        query = (
            OfflineDatabaseManager.offline_database_helper.generate_insert_bulk_query(
                self.schema_class, relation_dicts, on_conflict_do_nothing
            )
        )
        self._session.execute(query)

    def delete_by_entitys(self, id_: int) -> None:
        self.execute(
            delete(self.schema_class).where(
                (RelationTable.predicate == id_) | (RelationTable.object == id_)
            )
        )
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        # self._session.flush()
        # await self._session.flush()
