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

    @staticmethod
    def _to_domain(relation_db: RuntimeRelationDB) -> RuntimeRelationInternal:
        relation_db_dict: dict = relation_db.model_dump()
        role_id: int = relation_db_dict.get("predicate")
        role_name = RoleCache.role_id_to_role_name_lookup[role_id]
        relation_db_dict.update(role=role_name)
        return RuntimeRelationInternal.model_validate(relation_db_dict)

    def _get_one_by_query(
        self, query: Select[tuple[RuntimeRelationTable]]
    ) -> RuntimeRelationInternal:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        relation_db = RuntimeRelationDB.model_validate(instance)
        return self._to_domain(relation_db)

    def _get_all_by_query(
        self, query: Select[tuple[RuntimeRelationTable]]
    ) -> List[RuntimeRelationInternal]:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        instances = result.scalars().all()
        relation_dbs = [
            RuntimeRelationDB.model_validate(instance) for instance in instances
        ]
        return list(map(self._to_domain, relation_dbs))

    def all(self) -> Generator[RuntimeRelationInternal, None, None]:
        for instance in self._all():
            # async for instance in self._all():
            yield RuntimeRelationInternal.model_validate(instance)

    def get(self, relation_id: int) -> RuntimeRelationDB:
        # print(f"get")
        query = select(RuntimeRelationTable).where(
            RuntimeRelationTable.id == relation_id
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)
        # print(f"result: {result}")

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError
        # print(f"instance: {instance}")
        return RuntimeRelationDB.model_validate(instance)

    # def get_random(self, role_names: List[str] = None) -> RelationInternal:
    #     role_ids = [
    #         RoleDataAccess.role_name_to_role_id_lookup[role_name]
    #         for role_name in role_names
    #     ]
    #     while True:
    #         n: float = random()
    #         where_clause = RelationTable.random > n
    #         if role_names:
    #             where_clause &= RelationTable.id.in_(role_ids)
    #         query = (
    #             select(RelationTable)
    #             .where(where_clause)
    #             .order_by(RelationTable.random, RelationTable.id)
    #             .limit(1)
    #         )
    #         result: Result = self.execute(query)
    #         instance = result.scalars().one_or_none()
    #         # relation = self._session.scalars(query).one_or_none()
    #         if instance:
    #             break
    #
    #     log.debug(f"random relation: {instance}")
    #     relation_db = RelationDB.model_validate(instance)
    #
    #     return self._to_domain(relation_db)

    def get_id_by_key(self, key: dict) -> int:
        # print(f"find_by_key")
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
        # print(f"find_by_id")
        query = (
            select(RuntimeRelationTable)
            .with_for_update(of=RuntimeRelationTable, nowait=True)
            .where(RuntimeRelationTable.id == relation_id)
        )
        return self._get_one_by_query(query)

    def find_by_key(self, key: dict) -> RuntimeRelationInternal:
        # print(f"find_by_key")
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

    # def find_by_type_and_ids_and_role_names(
    #     self,
    #     lh_type: EntityType,
    #     lh_ids: List[int],
    #     rh_type: EntityType,
    #     rh_ids: List[int],
    #     role_names: List[str],
    # ) -> List[Relation]:
    #     where_clause = RelationTable.entity_one_type == lh_type
    #     where_clause &= RelationTable.entity_two_type == rh_type
    #     where_clause &= RelationTable.entity_one_id.in_(lh_ids)
    #     where_clause &= RelationTable.entity_two_id.in_(rh_ids)
    #     if role_names:
    #         role_ids = [
    #             RoleDataAccess.role_name_to_role_id_lookup[role_name]
    #             for role_name in role_names
    #         ]
    #         where_clause &= RelationTable.role_id.in_(role_ids)
    #     # TODO search by year
    #     # if year is not None:
    #     #     year_clause = cls.year.is_null(True)
    #     #     if isinstance(year, int):
    #     #         year_clause |= cls.year == year
    #     #     else:
    #     #         year_clause |= cls.year.between(year[0], year[1])
    #     #     where_clause &= year_clause
    #     query = select(RelationTable).where(where_clause)
    #     return self._get_all_by_query(query)

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

    # def create_and_get_id(self, relation: RelationUncommitted) -> int:
    #     relation_payload = relation.model_dump(exclude={"role_name"})
    #     role_id = RoleDataAccess.role_name_to_role_id_lookup[relation.role_name]
    #     relation_payload.update(id=role_id)
    #     saved_relation: RelationTable = self._save(relation_payload)
    #     return saved_relation.id
    #     # saved_relation: Relation = await repository.get(order_flat.id)
    #     # return Relation.model_validate(instance)

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
        # print(f"relation_db: {utils.normalize_dict(relation_db)}")
        return self._to_domain(relation_db)

    def create_bulk(
        self, relations: List[RuntimeRelationUncommitted], on_conflict_do_nothing=False
    ) -> None:
        from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

        relation_dicts = []
        for relation in relations:
            relation_dict = relation.model_dump(exclude={"role_name"})
            role_id = RoleCache.role_name_to_role_id_lookup[relation.role_name]
            relation_dict.update(predicate=role_id)
            # print(f"relation_dict: {relation_dict}")
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

    # def delete_by_entity_id_and_entity_type(
    #     self, entity_id: int, entity_type: EntityType
    # ) -> None:
    #     self.execute(
    #         delete(self.schema_class).where(
    #             (
    #                 (RelationTable.entity_one_id == entity_id)
    #                 & (RelationTable.entity_one_type == entity_type)
    #             )
    #             | (
    #                 (RelationTable.entity_two_id == entity_id)
    #                 & (RelationTable.entity_two_type == entity_type)
    #             )
    #         )
    #     )
    #     # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
    #     # self._session.flush()
    #     # await self._session.flush()
