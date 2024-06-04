import logging
from random import random
from typing import Generator, Any, List, Tuple, cast

from sqlalchemy import Result, select, update, Select

from discograph import utils
from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.relation_table import RelationTable
from discograph.library.database.role_repository import RoleRepository
from discograph.library.domain.entity import Entity
from discograph.library.domain.relation import Relation, RelationUncommitted, RelationDB
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.role_type import RoleType

log = logging.getLogger(__name__)


class RelationRepository(BaseRepository[RelationTable]):
    schema_class = RelationTable

    @staticmethod
    def _to_domain(relation_db: RelationDB) -> Relation:
        # print(f"_to_domain")
        relation_db_dict: dict = relation_db.model_dump()
        # print(f"original relation_db_dict: {relation_db_dict}")
        role_id: int = relation_db_dict.get("role_id")
        # print(f"role_id: {role_id}")
        role_name = RoleType.role_id_to_role_name_lookup[role_id]
        # print(f"role_name: {role_name}")
        relation_db_dict.update(role=role_name)

        # print(f"updated relation_db_dict: {relation_db_dict}")
        return Relation.model_validate(relation_db_dict)

    # @staticmethod
    # def _to_domain(relation_db: RelationDB) -> Relation:
    #     # print(f"_to_domain")
    #     relation_db_dict: dict = relation_db.model_dump(
    #         exclude={"releases": {"artists"}}
    #     )
    #     # print(f"original relation_db_dict: {relation_db_dict}")
    #     role: Role = relation_db_dict.get("role")
    #     # print(f"role: {role}")
    #     role_name = role.get("role_name") if role is not None else None
    #     relation_db_dict.update(role=role_name)
    #
    #     releases_list = relation_db_dict.get("releases")
    #     # print(f"original releases: {releases_list}")
    #     releases = {}
    #     for release in releases_list:
    #         key = str(release.get("release_id"))
    #         value = release.get("release_date")
    #         if value is not None:
    #             if isinstance(value, int):
    #                 pass
    #             elif isinstance(value, date):
    #                 value = value.year
    #         releases[key] = value
    #
    #     # print(f"updated releases: {releases}")
    #     relation_db_dict.update(releases=releases)
    #     # print(f"updated relation_db_dict: {relation_db_dict}")
    #     return Relation.model_validate(relation_db_dict)

    def _get_one_by_query(self, query: Select[tuple[RelationTable]]) -> Relation:
        # print(f"_get_one_by_query")
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        # print(f"instance: {instance}")

        relation_db = RelationDB.model_validate(instance)
        # print(f"relation_db: {utils.normalize_dict(relation_db)}")
        return self._to_domain(relation_db)

    def _get_all_by_query(self, query: Select[tuple[RelationTable]]) -> List[Relation]:
        # print(f"_get_all_by_query")
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
        query = (
            select(RelationTable)
            # .options(
            #     joinedload(RelationTable.role),
            # )
            .where(RelationTable.relation_id == relation_id)
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)
        # print(f"result: {result}")

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError
        # print(f"instance: {instance}")
        return RelationDB.model_validate(instance)

    # def get(self, relation_id: int) -> RelationDB:
    #     # print(f"get")
    #     query = (
    #         select(RelationTable)
    #         .options(
    #             joinedload(RelationTable.role),
    #         )
    #         # .execution_options(populate_existing=True)
    #         .options(
    #             # Many to Many relationship, only load release id and date columns
    #             selectinload(RelationTable.releases)
    #             # .load_only(
    #             #     ReleaseTable.release_date
    #             # )
    #         )
    #         .where(RelationTable.relation_id == relation_id)
    #     )
    #     result: Result = self.execute(query)
    #     # result: Result = await self.execute(query)
    #     # print(f"result: {result}")
    #
    #     if not (instance := result.scalars().one_or_none()):
    #         raise NotFoundError
    #     # print(f"instance: {instance}")
    #     return RelationDB.model_validate(instance)

    def get_id_by_key(self, key: dict) -> int:
        # print(f"find_by_key")
        if "role_id" not in key:
            if "role_name" in key:
                role_name = key["role_name"]
                key["role_id"] = RoleType.role_name_to_role_id_lookup[role_name]
                # role = RoleRepository().get_by_name(key["role_name"])
                # key["role_id"] = role.role_id
        query = (
            select(RelationTable.relation_id)
            # .options(
            #     joinedload(RelationTable.role),
            # )
            # .execution_options(populate_existing=True)
            # .options(
            #     # Many to Many relationship, only load release id and date columns
            #     selectinload(RelationTable.releases)
            #     # .load_only(
            #     #     ReleaseTable.release_date
            #     # )
            # )
            .where(
                (RelationTable.entity_one_id == key["entity_one_id"])
                & (RelationTable.entity_one_type == key["entity_one_type"])
                & (RelationTable.entity_two_id == key["entity_two_id"])
                & (RelationTable.entity_two_type == key["entity_two_type"])
                & (RelationTable.role_id == key["role_id"])
            )
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
            # .options(
            #     joinedload(RoleTable.role_id),
            # )
            .where(RelationTable.relation_id == relation_id)
        )
        return self._get_one_by_query(query)

    # def find_by_id(self, relation_id: int) -> Relation:
    #     # print(f"find_by_id")
    #     query = (
    #         select(RelationTable)
    #         .options(
    #             joinedload(RoleTable.role_id),
    #         )
    #         .options(
    #             # Many to Many relationship, only load release id and date columns
    #             selectinload(RelationTable.releases)
    #             # .load_only(
    #             #     ReleaseTable.release_date
    #             # )
    #         )
    #         .where(RelationTable.relation_id == relation_id)
    #     )
    #     return self._get_one_by_query(query)

    def find_by_key(self, key: dict) -> Relation:
        # print(f"find_by_key")
        if "role_id" not in key:
            if "role_name" in key:
                role_name = key["role_name"]
                key["role_id"] = RoleType.role_name_to_role_id_lookup[role_name]
                # print(f"find_by_key role_id: {key['role_id']}")
                assert key["role_id"] is not None
                # role = RoleRepository().get_by_name(key["role_name"])
                # key["role_id"] = role.role_id
            elif "role" in key:
                role_name = key["role"]
                key["role_id"] = RoleType.role_name_to_role_id_lookup[role_name]
                # role = RoleRepository().get_by_name(key["role"])
                # key["role_id"] = role.role_id
        query = (
            select(RelationTable)
            # .options(
            #     joinedload(RelationTable.role),
            # )
            .where(
                (RelationTable.entity_one_id == key["entity_one_id"])
                & (RelationTable.entity_one_type == key["entity_one_type"])
                & (RelationTable.entity_two_id == key["entity_two_id"])
                & (RelationTable.entity_two_type == key["entity_two_type"])
                & (RelationTable.role_id == key["role_id"])
            )
        )
        return self._get_one_by_query(query)

    # def find_by_key(self, key: dict) -> Relation:
    #     # print(f"find_by_key")
    #     if "role_id" not in key:
    #         if "role_name" in key:
    #             role = RoleRepository().get_by_name(key["role_name"])
    #             key["role_id"] = role.role_id
    #         elif "role" in key:
    #             role = RoleRepository().get_by_name(key["role"])
    #             key["role_id"] = role.role_id
    #     query = (
    #         select(RelationTable)
    #         .options(
    #             joinedload(RelationTable.role),
    #         )
    #         # .execution_options(populate_existing=True)
    #         .options(
    #             # Many to Many relationship, only load release id and date columns
    #             selectinload(RelationTable.releases)
    #             # .load_only(
    #             #     ReleaseTable.release_date
    #             # )
    #         )
    #         .where(
    #             (RelationTable.entity_one_id == key["entity_one_id"])
    #             & (RelationTable.entity_one_type == key["entity_one_type"])
    #             & (RelationTable.entity_two_id == key["entity_two_id"])
    #             & (RelationTable.entity_two_type == key["entity_two_type"])
    #             & (RelationTable.role_id == key["role_id"])
    #         )
    #     )
    #     return self._get_one_by_query(query)

    def find_by_entity_one_key(
        self,
        entity_id: int,
        entity_type: EntityType,
    ) -> List[Relation]:
        query = (
            select(RelationTable)
            # .options(
            #     joinedload(RelationTable.role),
            # )
            .where(
                (RelationTable.entity_one_id == entity_id)
                & (RelationTable.entity_one_type == entity_type)
            )
        )
        return self._get_all_by_query(query)

    def find_by_entity_two_key(
        self,
        entity_id: int,
        entity_type: EntityType,
    ) -> List[Relation]:
        query = (
            select(RelationTable)
            # .options(
            #     joinedload(RelationTable.role),
            # )
            .where(
                (RelationTable.entity_two_id == entity_id)
                & (RelationTable.entity_two_type == entity_type)
            )
        )
        return self._get_all_by_query(query)

    def find_by_type_and_ids_and_role_names(
        self,
        lh_type: EntityType,
        lh_ids: List[int],
        rh_type: EntityType,
        rh_ids: List[int],
        role_names,
    ) -> List[Relation]:
        where_clause = RelationTable.entity_one_type == lh_type
        where_clause &= RelationTable.entity_two_type == rh_type
        where_clause &= RelationTable.entity_one_id.in_(lh_ids)
        where_clause &= RelationTable.entity_two_id.in_(rh_ids)
        if role_names:
            roles = [
                RoleRepository().get_by_name(role_name) for role_name in role_names
            ]
            where_clause &= RelationTable.role.in_(roles)
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
            # .options(
            #     joinedload(RelationTable.role),
            # )
            .where(where_clause)
        )
        return self._get_all_by_query(query)
        # relation_result = self._session.scalars(
        #     select(RelationTable).where(where_clause)
        # ).one()
        # return relation_result

    def create_and_get_id(self, relation: RelationUncommitted) -> int:
        relation_payload = relation.model_dump(exclude={"role_name"})
        role_id = RoleType.role_name_to_role_id_lookup[relation.role_name]
        relation_payload.update(role_id=role_id)
        # role = RoleRepository().get_by_name(relation.role_name)
        # relation_payload.update(role_id=role.role_id)
        saved_relation: RelationTable = self._save(relation_payload)
        return saved_relation.relation_id
        # saved_relation: Relation = await repository.get(order_flat.id)
        # return Relation.model_validate(instance)

    def create(self, relation: RelationUncommitted) -> Relation:
        relation_payload = relation.model_dump(exclude={"role_name"})
        role_id = RoleType.role_name_to_role_id_lookup[relation.role_name]
        relation_payload.update(role_id=role_id)
        # role = RoleRepository().get_by_name(relation.role_name)
        # relation_payload.update(role_id=role.role_id)
        saved_relation: RelationTable = self._save(relation_payload)
        # print(f"saved_relation: {saved_relation}")
        # instance: RelationTable = await self._save(schema.model_dump())
        # retrieved_relation = self.get(saved_relation.relation_id)
        # print(f"retrieved_relation: {retrieved_relation}")
        relation_db = RelationDB.model_validate(saved_relation)
        return self._to_domain(relation_db)
        # saved_relation: Relation = await repository.get(order_flat.id)
        # return Relation.model_validate(instance)

    def get_chunked_relation_ids(self, concurrency_multiplier=1) -> List[Tuple[Any]]:
        # TODO handle session and errors
        from discograph.database import get_concurrency_count

        all_ids = self._session.scalars(
            select(RelationTable.relation_id).order_by(RelationTable.relation_id)
        ).all()

        num_chunks = get_concurrency_count() * concurrency_multiplier
        return utils.split_tuple(num_chunks, all_ids)

    def get_relations_for_entity(self, entity: Entity) -> List[Relation]:
        if entity is None:
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
                    (RelationTable.entity_one_id == entity.entity_id)
                    & (RelationTable.entity_one_type == entity.entity_type)
                )
                | (
                    (RelationTable.entity_two_id == entity.entity_id)
                    & (RelationTable.entity_two_type == entity.entity_type)
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
        relations = self._get_all_by_query(query)
        return relations
        # data = []
        # for relation in relations:
        #     # category = RoleType.role_definitions[relation.role]
        #     # if category is None:
        #     #     continue
        #     log.debug(f"relation.role_id: {relation.role_id}")
        #     datum = {
        #         "role": RoleType.role_id_to_role_name_lookup[relation.role_id],
        #     }
        #     data.append(datum)
        # data = {"results": tuple(data)}
        # return data

    def get_random_relation(self, roles: list[str] = None):
        # TODO handle session and errors
        while True:
            n: float = random()
            where_clause = RelationTable.random > n
            if roles:
                where_clause &= RelationTable.role.in_(roles)
            relation = self._session.scalars(
                select(RelationTable)
                .where(where_clause)
                .order_by(RelationTable.random, RelationTable.role)
                .limit(1)
            ).one_or_none()
            if relation:
                break

        log.debug(f"random relation: {relation}")
        return relation

    def update(
        self,
        relation_id: int,
        version_id: int,
        payload: dict[str, Any],
    ) -> Relation:
        """Updates an existed instance of the model in the related table.
        If some data is not exist in the payload then the null value will
        be passed to the schema class."""

        query = (
            update(self.schema_class)
            .where(
                (RelationTable.relation_id == relation_id)
                & (RelationTable.version_id == version_id)
            )
            .values(payload)
            .returning(self.schema_class)
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)
        # self._session.flush()
        # await self._session.flush()

        if not (instance := result.scalar_one_or_none()):
            raise DatabaseError

        relation_db = RelationDB.model_validate(instance)
        # print(f"relation_db: {utils.normalize_dict(relation_db)}")
        return self._to_domain(relation_db)

    # def update(
    #     self,
    #     key: dict[str, Any],
    #     payload: dict[str, Any],
    # ) -> RelationTable:
    #     """Updates an existed instance of the model in the related table.
    #     If some data is not exist in the payload then the null value will
    #     be passed to the schema class."""
    #
    #     query = (
    #         update(self.schema_class)
    #         .where(
    #             (RelationTable.entity_one_type == key["entity_one_type"])
    #             & (RelationTable.entity_one_id == key["entity_one_id"])
    #             & (RelationTable.entity_two_type == key["entity_two_type"])
    #             & (RelationTable.entity_two_id == key["entity_two_id"])
    #             & (RelationTable.role_id == key["role_id"])
    #         )
    #         .values(payload)
    #         .returning(self.schema_class)
    #     )
    #     result: Result = self.execute(query)
    #     # result: Result = await self.execute(query)
    #     self._session.flush()
    #     # await self._session.flush()
    #
    #     if not (schema := result.scalar_one_or_none()):
    #         raise DatabaseError
    #
    #     return schema

    # def delete_relation_duplicates(self) -> None:
    #     with transaction():
    #         self._session.execute(
    #             text(
    #                 f"DELETE FROM {RelationTable.__tablename__} "
    #                 + f"WHERE relation_id IN ( "
    #                 + f"  SELECT a.relation_id FROM {RelationTable.__tablename__} a "
    #                 + f"  JOIN {RelationTable.__tablename__} b "
    #                 + f"  ON (a.entity_one_id = b.entity_one_id "
    #                 + f"  AND a.entity_one_type = b.entity_one_type "
    #                 + f"  AND a.entity_two_id = b.entity_two_id "
    #                 + f"  AND a.entity_two_type = b.entity_two_type "
    #                 + f"  AND a.role_id = b.role_id) "
    #                 + f"  WHERE a.relation_id > b.relation_id);"
    #             )
    #         )

    # def delete_relation_duplicates(self) -> None:
    #     with transaction():
    #         self._session.execute(
    #             text(
    #                 f"DELETE FROM {RelationTable.__tablename__} a "
    #                 + f"USING {RelationTable.__tablename__} b "
    #                 + f"WHERE a.relation_id > b.relation_id "
    #                 + f"AND a.entity_one_id = b.entity_one_id "
    #                 + f"AND a.entity_one_type = b.entity_one_type "
    #                 + f"AND a.entity_two_id = b.entity_two_id "
    #                 + f"AND a.entity_two_type = b.entity_two_type "
    #                 + f"AND a.role_id = b.role_id;"
    #             )
    #         )
    # delete from TABLE where id in (
    #     select TABLE.id from TABLE q1
    # join TABLE q2 on q1.name = q2.name
    # where q1.id < q2.id)

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
                RoleType.role_name_to_role_id_lookup[role_name]
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
