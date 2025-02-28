import logging
from typing import Generator, List

from sqlalchemy import Result, select, Select, delete

from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.cache.role_cache import RoleCache
from discograph.runtime.runtime_database import RuntimeRelationTable
from discograph.runtime.runtime_database.runtime_base_repository import (
    RuntimeBaseRepository,
)
from discograph.runtime.runtime_domain.relation import (
    RuntimeRelationDB,
    RuntimeRelationInternal,
    RuntimeRelationUncommitted,
)

log = logging.getLogger(__name__)


class RuntimeRelationRepository(RuntimeBaseRepository[RuntimeRelationTable]):
    schema_class = RuntimeRelationTable

    def _get_one_by_query(
        self, query: Select[tuple[RuntimeRelationTable]]
    ) -> RuntimeRelationInternal:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        relation_db = RuntimeRelationDB.model_validate(instance)
        return relation_db.to_domain()

    def _get_all_by_query(
        self, query: Select[tuple[RuntimeRelationTable]]
    ) -> List[RuntimeRelationInternal]:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        instances = result.scalars().all()
        relation_dbs = [
            RuntimeRelationDB.model_validate(instance) for instance in instances
        ]
        relations = [relation_db.to_domain() for relation_db in relation_dbs]
        return relations

    def all(self) -> Generator[RuntimeRelationInternal, None, None]:
        for instance in self._all():
            # async for instance in self._all():
            yield RuntimeRelationInternal.model_validate(instance)

    def get(self, relation_id: int) -> RuntimeRelationDB:
        query = select(RuntimeRelationTable).where(
            RuntimeRelationTable.id == relation_id
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError
        return RuntimeRelationDB.model_validate(instance)

    def get_id_by_key(self, key: dict) -> int:
        query = select(RuntimeRelationTable.id).where(
            (RuntimeRelationTable.subject == key["subject"])
            & (RuntimeRelationTable.predicate == key["role_id"])
            & (RuntimeRelationTable.object == key["object"])
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalar()):
            raise NotFoundError
        return instance

    def find_by_id(self, relation_id: int) -> RuntimeRelationInternal:
        query = (
            select(RuntimeRelationTable)
            .with_for_update(of=RuntimeRelationTable, nowait=True)
            .where(RuntimeRelationTable.id == relation_id)
        )
        return self._get_one_by_query(query)

    def find_by_key(self, key: dict) -> RuntimeRelationInternal:
        if "role_id" not in key:
            if "role_name" in key:
                role_name = key["role_name"]
                key["role_id"] = RoleCache.role_name_to_role_id_lookup[role_name]
            elif "role" in key:
                role_name = key["role"]
                key["role_id"] = RoleCache.role_name_to_role_id_lookup[role_name]
        query = select(RuntimeRelationTable).where(
            (RuntimeRelationTable.subject == key["subject"])
            & (RuntimeRelationTable.predicate == key["role_id"])
            & (RuntimeRelationTable.object == key["object"])
        )
        return self._get_one_by_query(query)

    def find_by_entity(self, id_: int) -> List[RuntimeRelationInternal]:
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
            select(RuntimeRelationTable)
            .where(
                (RuntimeRelationTable.subject == id_)
                | (RuntimeRelationTable.object == id_)
            )
            .order_by(
                RuntimeRelationTable.predicate,
                RuntimeRelationTable.subject,
                RuntimeRelationTable.object,
            )
        )
        return self._get_all_by_query(query)

    def find_by_entity_and_roles(
        self, id_: int, role_ids: list[int]
    ) -> List[RuntimeRelationInternal]:
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
            select(RuntimeRelationTable)
            .where(
                (
                    (RuntimeRelationTable.subject == id_)
                    | (RuntimeRelationTable.object == id_)
                )
                & (RuntimeRelationTable.predicate.in_(role_ids))
            )
            .order_by(
                RuntimeRelationTable.predicate,
                RuntimeRelationTable.subject,
                RuntimeRelationTable.object,
            )
        )
        return self._get_all_by_query(query)

    def create(
        self, relation: RuntimeRelationUncommitted, on_conflict_do_nothing=False
    ) -> RuntimeRelationInternal:
        from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

        relation_dict = relation.model_dump(exclude={"role_name"})
        role_id = RoleCache.role_name_to_role_id_lookup[relation.role_name]
        relation_dict.update(predicate=role_id)
        query = RuntimeDatabaseManager.runtime_database_helper.generate_insert_query(
            self.schema_class, relation_dict, on_conflict_do_nothing
        )
        result: Result = self._session.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (instance := result.scalar_one_or_none()):
            raise DatabaseError

        relation_db = RuntimeRelationDB.model_validate(instance)
        return relation_db.to_domain()

    def create_bulk(
        self, relations: List[RuntimeRelationUncommitted], on_conflict_do_nothing=False
    ) -> None:
        from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

        relation_dicts = []
        for relation in relations:
            relation_dict = relation.model_dump(exclude={"role_name"})
            role_id = RoleCache.role_name_to_role_id_lookup[relation.role_name]
            relation_dict.update(predicate=role_id)
            relation_dicts.append(relation_dict)
        query = (
            RuntimeDatabaseManager.runtime_database_helper.generate_insert_bulk_query(
                self.schema_class, relation_dicts, on_conflict_do_nothing
            )
        )
        self._session.execute(query)

    def delete_by_entitys(self, id_: int) -> None:
        self.execute(
            delete(self.schema_class).where(
                (RuntimeRelationTable.predicate == id_)
                | (RuntimeRelationTable.object == id_)
            )
        )
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        # self._session.flush()
        # await self._session.flush()
