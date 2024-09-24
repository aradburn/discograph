import logging
from random import random
from typing import Generator, List, Tuple, cast

from sqlalchemy import Result, select, Select, delete

from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.relation_table import RelationTable
from discograph.library.domain.relation import Relation, RelationUncommitted, RelationDB
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class RelationRepository(BaseRepository[RelationTable]):
    schema_class = RelationTable

    @staticmethod
    def _to_domain(relation_db: RelationDB) -> Relation:
        relation_db_dict: dict = relation_db.model_dump()
        role_id: int = relation_db_dict.get("role_id")
        role_name = RoleDataAccess.role_id_to_role_name_lookup[role_id]
        relation_db_dict.update(role=role_name)
        return Relation.model_validate(relation_db_dict)

    def _get_one_by_query(self, query: Select[tuple[RelationTable]]) -> Relation:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        relation_db = RelationDB.model_validate(instance)
        return self._to_domain(relation_db)

    def _get_all_by_query(self, query: Select[tuple[RelationTable]]) -> List[Relation]:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        instances = result.scalars().all()
        relation_dbs = [RelationDB.model_validate(instance) for instance in instances]
        return list(map(self._to_domain, relation_dbs))

    def all(self) -> Generator[Relation, None, None]:
        for instance in self._all():
            # async for instance in self._all():
            yield Relation.model_validate(instance)

    def get(self, relation_id: int) -> RelationDB:
        # print(f"get")
        query = select(RelationTable).where(RelationTable.relation_id == relation_id)
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)
        # print(f"result: {result}")

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError
        # print(f"instance: {instance}")
        return RelationDB.model_validate(instance)

    def get_random(self, role_names: List[str] = None) -> Relation:
        role_ids = [
            RoleDataAccess.role_name_to_role_id_lookup[role_name]
            for role_name in role_names
        ]
        while True:
            n: float = random()
            where_clause = RelationTable.random > n
            if role_names:
                where_clause &= RelationTable.role_id.in_(role_ids)
            query = (
                select(RelationTable)
                .where(where_clause)
                .order_by(RelationTable.random, RelationTable.role_id)
                .limit(1)
            )
            result: Result = self.execute(query)
            instance = result.scalars().one_or_none()
            # relation = self._session.scalars(query).one_or_none()
            if instance:
                break

        log.debug(f"random relation: {instance}")
        relation_db = RelationDB.model_validate(instance)

        return self._to_domain(relation_db)

    def get_id_by_key(self, key: dict) -> int:
        # print(f"find_by_key")
        if "role_id" not in key:
            if "role_name" in key:
                role_name = key["role_name"]
                key["role_id"] = RoleDataAccess.role_name_to_role_id_lookup[role_name]
        query = select(RelationTable.relation_id).where(
            (RelationTable.entity_one_id == key["entity_one_id"])
            & (RelationTable.entity_one_type == key["entity_one_type"])
            & (RelationTable.entity_two_id == key["entity_two_id"])
            & (RelationTable.entity_two_type == key["entity_two_type"])
            & (RelationTable.role_id == key["role_id"])
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalar()):
            raise NotFoundError
        return instance

    def find_by_id(self, relation_id: int) -> Relation:
        # print(f"find_by_id")
        query = (
            select(RelationTable)
            .with_for_update(of=RelationTable, nowait=True)
            .where(RelationTable.relation_id == relation_id)
        )
        return self._get_one_by_query(query)

    def find_by_key(self, key: dict) -> Relation:
        # print(f"find_by_key")
        if "role_id" not in key:
            if "role_name" in key:
                role_name = key["role_name"]
                key["role_id"] = RoleDataAccess.role_name_to_role_id_lookup[role_name]
                # print(f"find_by_key role_id: {key['role_id']}")
                assert key["role_id"] is not None
            elif "role" in key:
                role_name = key["role"]
                key["role_id"] = RoleDataAccess.role_name_to_role_id_lookup[role_name]
        query = (
            select(RelationTable).where(
                (RelationTable.entity_one_id == key["entity_one_id"])
                & (RelationTable.entity_one_type == key["entity_one_type"])
                & (RelationTable.entity_two_id == key["entity_two_id"])
                & (RelationTable.entity_two_type == key["entity_two_type"])
                & (RelationTable.role_id == key["role_id"])
            )
            # .with_for_update(of=RelationTable, nowait=True)
        )
        return self._get_one_by_query(query)

    def find_by_entity_one_key(
        self,
        entity_id: int,
        entity_type: EntityType,
    ) -> List[Relation]:
        query = select(RelationTable).where(
            (RelationTable.entity_one_id == entity_id)
            & (RelationTable.entity_one_type == entity_type)
        )
        return self._get_all_by_query(query)

    def find_by_entity_two_key(
        self,
        entity_id: int,
        entity_type: EntityType,
    ) -> List[Relation]:
        query = select(RelationTable).where(
            (RelationTable.entity_two_id == entity_id)
            & (RelationTable.entity_two_type == entity_type)
        )
        return self._get_all_by_query(query)

    def find_by_type_and_ids_and_role_names(
        self,
        lh_type: EntityType,
        lh_ids: List[int],
        rh_type: EntityType,
        rh_ids: List[int],
        role_names: List[str],
    ) -> List[Relation]:
        where_clause = RelationTable.entity_one_type == lh_type
        where_clause &= RelationTable.entity_two_type == rh_type
        where_clause &= RelationTable.entity_one_id.in_(lh_ids)
        where_clause &= RelationTable.entity_two_id.in_(rh_ids)
        if role_names:
            role_ids = [
                RoleDataAccess.role_name_to_role_id_lookup[role_name]
                for role_name in role_names
            ]
            where_clause &= RelationTable.role_id.in_(role_ids)
        # TODO search by year
        # if year is not None:
        #     year_clause = cls.year.is_null(True)
        #     if isinstance(year, int):
        #         year_clause |= cls.year == year
        #     else:
        #         year_clause |= cls.year.between(year[0], year[1])
        #     where_clause &= year_clause
        query = select(RelationTable).where(where_clause)
        return self._get_all_by_query(query)

    def find_by_entity_id_and_entity_type(
        self, entity_id: int, entity_type: EntityType
    ) -> List[Relation]:
        if entity_id is None or entity_type is None:
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
                (
                    (RelationTable.entity_one_id == entity_id)
                    & (RelationTable.entity_one_type == entity_type)
                )
                | (
                    (RelationTable.entity_two_id == entity_id)
                    & (RelationTable.entity_two_type == entity_type)
                )
            )
            .order_by(
                RelationTable.role_id,
                RelationTable.entity_one_id,
                RelationTable.entity_one_type,
                RelationTable.entity_two_id,
                RelationTable.entity_two_type,
            )
        )
        return self._get_all_by_query(query)

    def create_and_get_id(self, relation: RelationUncommitted) -> int:
        relation_payload = relation.model_dump(exclude={"role_name"})
        role_id = RoleDataAccess.role_name_to_role_id_lookup[relation.role_name]
        relation_payload.update(role_id=role_id)
        saved_relation: RelationTable = self._save(relation_payload)
        return saved_relation.relation_id
        # saved_relation: Relation = await repository.get(order_flat.id)
        # return Relation.model_validate(instance)

    def create(
        self, relation: RelationUncommitted, on_conflict_do_nothing=False
    ) -> Relation:
        from discograph.library.database.database_helper import DatabaseHelper

        relation_dict = relation.model_dump(exclude={"role_name"})
        role_id = RoleDataAccess.role_name_to_role_id_lookup[relation.role_name]
        relation_dict.update(role_id=role_id)
        # relation_dict.update(version_id=1)
        query = DatabaseHelper.db_helper.generate_insert_query(
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
        return self._to_domain(relation_db)

    def create_bulk(
        self, relations: List[RelationUncommitted], on_conflict_do_nothing=False
    ) -> None:
        from discograph.library.database.database_helper import DatabaseHelper

        relation_dicts = []
        for relation in relations:
            relation_dict = relation.model_dump(exclude={"role_name"})
            role_id = RoleDataAccess.role_name_to_role_id_lookup[relation.role_name]
            relation_dict.update(role_id=role_id)
            # relation_dict.update(version_id=1)
            relation_dicts.append(relation_dict)
        query = DatabaseHelper.db_helper.generate_insert_bulk_query(
            self.schema_class, relation_dicts, on_conflict_do_nothing
        )
        # query = (
        #     insert(self.schema_class).values(relation_dict).returning(self.schema_class)
        # )
        self._session.execute(query)
        # result: Result = self._session.execute(query)
        # result: Result = await self.execute(query)
        # self._session.flush()
        # await self._session.flush()

        # if not (instance := result.scalar_one_or_none()):
        #     raise DatabaseError

        # relation_db = RelationDB.model_validate(instance)
        # print(f"relation_db: {utils.normalize_dict(relation_db)}")
        # return self._to_domain(relation_db)

    def delete_by_entity_id_and_entity_type(
        self, entity_id: int, entity_type: EntityType
    ) -> None:
        self.execute(
            delete(self.schema_class).where(
                (
                    (RelationTable.entity_one_id == entity_id)
                    & (RelationTable.entity_one_type == entity_type)
                )
                | (
                    (RelationTable.entity_two_id == entity_id)
                    & (RelationTable.entity_two_type == entity_type)
                )
            )
        )
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        # self._session.flush()
        # await self._session.flush()

    # def set(
    #     self,
    #     key: str,
    #     value: str,
    # ) -> None:
    #     self._session.execute(text(f"set {key} = {value};"))

    def search_multi(
        self, *, entity_keys: List[Tuple[int, EntityType]], role_names: List[str]
    ) -> List[Relation]:
        assert entity_keys
        artist_ids, label_ids = [], []
        log.debug(f"entity_keys: {entity_keys}")
        log.debug(f"role_names: {role_names}")

        for entity_id, entity_type in entity_keys:
            if entity_type == EntityType.ARTIST:
                artist_ids.append(entity_id)
            elif entity_type == EntityType.LABEL:
                label_ids.append(entity_id)
        log.debug(f"artist_ids: {artist_ids}")
        log.debug(f"label_ids: {label_ids}")

        if len(artist_ids) > 0:
            artist_where_clause = (
                cast(
                    "ColumnElement[bool]",
                    RelationTable.entity_one_type == EntityType.ARTIST,
                )
                & cast(
                    "ColumnElement[bool]", RelationTable.entity_one_id.in_(artist_ids)
                )
            ) | (
                cast(
                    "ColumnElement[bool]",
                    RelationTable.entity_two_type == EntityType.ARTIST,
                )
                & cast(
                    "ColumnElement[bool]", RelationTable.entity_two_id.in_(artist_ids)
                )
            )
        else:
            artist_where_clause = None
        if len(label_ids) > 0:
            label_where_clause = (
                cast(
                    "ColumnElement[bool]",
                    RelationTable.entity_one_type == EntityType.LABEL,
                )
                & cast(
                    "ColumnElement[bool]", RelationTable.entity_one_id.in_(label_ids)
                )
            ) | (
                cast(
                    "ColumnElement[bool]",
                    RelationTable.entity_two_type == EntityType.LABEL,
                )
                & cast(
                    "ColumnElement[bool]", RelationTable.entity_two_id.in_(label_ids)
                )
            )
        else:
            label_where_clause = None
        if artist_ids and label_ids:
            where_clause = artist_where_clause | label_where_clause
        elif artist_ids:
            where_clause = artist_where_clause
        elif label_ids:
            where_clause = label_where_clause
        else:
            where_clause = None
        if role_names:
            role_ids = [
                RoleDataAccess.role_name_to_role_id_lookup[role_name]
                for role_name in role_names
            ]
            if where_clause:
                where_clause = where_clause & (RelationTable.role_id.in_(role_ids))
            else:
                where_clause = RelationTable.role_id.in_(role_ids)
        log.debug(f"where_clause: {where_clause}")
        query = select(RelationTable).where(where_clause)
        relations = self._get_all_by_query(query)
        return relations

    # noinspection PyUnusedLocal
    def search_bimulti(
        self,
        lh_entities: List[Tuple[int, EntityType]],
        rh_entities: List[Tuple[int, EntityType]],
        role_names: List[str] = None,
        year=None,
        verbose=True,
    ) -> List[Relation]:
        lh_artist_ids = []
        lh_label_ids = []
        rh_artist_ids = []
        rh_label_ids = []
        for entity_id, entity_type in lh_entities:
            if entity_type == EntityType.ARTIST:
                lh_artist_ids.append(entity_id)
            else:
                lh_label_ids.append(entity_id)
        for entity_id, entity_type in rh_entities:
            if entity_type == EntityType.ARTIST:
                rh_artist_ids.append(entity_id)
            else:
                rh_label_ids.append(entity_id)
        relations: List[Relation] = []
        if lh_artist_ids:
            lh_type = EntityType.ARTIST
            lh_ids = lh_artist_ids
            if rh_artist_ids:
                rh_type = EntityType.ARTIST
                rh_ids = rh_artist_ids
                results1 = self.find_by_type_and_ids_and_role_names(
                    lh_type, lh_ids, rh_type, rh_ids, role_names
                )
                relations.extend(results1)
            if rh_label_ids:
                rh_type = EntityType.LABEL
                rh_ids = rh_label_ids
                results2 = self.find_by_type_and_ids_and_role_names(
                    lh_type, lh_ids, rh_type, rh_ids, role_names
                )
                relations.extend(results2)
        if lh_label_ids:
            lh_type = EntityType.LABEL
            lh_ids = lh_label_ids
            if rh_artist_ids:
                rh_type = EntityType.ARTIST
                rh_ids = rh_artist_ids
                results3 = self.find_by_type_and_ids_and_role_names(
                    lh_type, lh_ids, rh_type, rh_ids, role_names
                )
                relations.extend(results3)
            if rh_label_ids:
                rh_type = EntityType.LABEL
                rh_ids = rh_label_ids
                results4 = self.find_by_type_and_ids_and_role_names(
                    lh_type, lh_ids, rh_type, rh_ids, role_names
                )
                relations.extend(results4)
        return relations
        # for query in queries:
        #     log.debug(f"search_bimulti query: {query}")
        #     relations.extend(query)
        # relation_links = {relation.link_key: relation for relation in relations}
        # log.debug(f"relation_links: {relation_links}")
        # return relation_links
