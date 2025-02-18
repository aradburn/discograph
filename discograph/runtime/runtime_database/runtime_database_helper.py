import logging
from abc import ABC, abstractmethod
from typing import Type, List, Any

from sqlalchemy import Engine, Index, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.dml import ReturningInsert, Insert

from discograph.config import Configuration, ENTITY_DETAILS_PATH, TEXT_SEARCH_PATH
from discograph.exceptions import NotFoundError
from discograph.library.cache.cache_manager import CacheManager
from discograph.library.fields.entity_id import to_entity_external_id
from discograph.library.fields.entity_type import EntityType
from discograph.library.full_text_search.entity_details_index import EntityDetailsIndex
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.runtime.data_access_layer.runtime_entity_data_access import (
    RuntimeEntityDataAccess,
)
from discograph.runtime.data_access_layer.runtime_role_data_access import (
    RuntimeRoleDataAccess,
)
from discograph.runtime.runtime_database.runtime_base_table import (
    RuntimeBase,
    RuntimeConcreteTable,
)
from discograph.runtime.runtime_database.runtime_entity_repository import (
    RuntimeEntityRepository,
)
from discograph.runtime.runtime_database.runtime_relation_repository import (
    RuntimeRelationRepository,
)

log = logging.getLogger(__name__)


class RuntimeDatabaseHelper(ABC):
    runtime_engine: Engine | None = None
    runtime_session_factory: sessionmaker | None = None
    flask_db_session: scoped_session | None = None

    idx_entity_one_id: Index | None = None
    idx_entity_two_id: Index | None = None

    text_search_index: TextSearchIndex | None = None
    entity_details_index: EntityDetailsIndex | None = None

    entity_count_cached = 0

    MAX_NODES = 400
    MAX_NODES_MOBILE = 25

    MAX_DEGREE = 5
    # was 12
    MAX_DEGREE_MOBILE = 3

    LINK_RATIO = 10
    # was 3

    @staticmethod
    @abstractmethod
    def setup_database(config: Configuration) -> Engine:
        pass

    @staticmethod
    @abstractmethod
    def shutdown_database() -> None:
        pass

    @classmethod
    def initialize(cls) -> None:
        """ensure the parent proc's database connections are not touched
        in the new connection pool"""
        from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

        RuntimeDatabaseManager.runtime_database_helper.runtime_engine.dispose(
            close=False
        )

    @staticmethod
    @abstractmethod
    def check_connection(config: Configuration, engine: Engine) -> None:
        pass

    @classmethod
    @abstractmethod
    def create_tables(cls, tables: List[str] = None) -> None:
        from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
        from discograph.runtime.runtime_database import ALL_RUNTIME_DATABASE_TABLES

        for table in ALL_RUNTIME_DATABASE_TABLES:
            log.debug(f"table definition for: {table.__tablename__}")
        for table in RuntimeBase.metadata.tables:
            log.debug(f"table in metadata: {table}")
        table_definitions: List[Table] = [
            RuntimeBase.metadata.tables[table_name] for table_name in tables
        ]
        for table in table_definitions:
            log.debug(f"creating table: {table.name}")
        RuntimeBase.metadata.create_all(
            RuntimeDatabaseManager.runtime_database_helper.runtime_engine,
            checkfirst=True,
            tables=table_definitions,
        )

    @classmethod
    @abstractmethod
    def drop_tables(cls, tables: List[str] = None) -> None:
        from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

        if tables is not None:
            table_definitions: List[Table] = [
                RuntimeBase.metadata.tables[table_name] for table_name in tables
            ]
            for table in table_definitions:
                log.debug(f"deleting table: {table.name}")
                table.drop(
                    RuntimeDatabaseManager.runtime_database_helper.runtime_engine,
                    checkfirst=True,
                )
        else:
            RuntimeBase.metadata.drop_all(
                RuntimeDatabaseManager.runtime_database_helper.runtime_engine,
                checkfirst=True,
            )

    @classmethod
    def load_tables(cls) -> None:
        log.info("Load tables")
        RuntimeRoleDataAccess.load_all_roles()
        RuntimeDatabaseHelper.entity_details_index = (
            RuntimeEntityDataAccess.load_entity_details_index_from_file(
                ENTITY_DETAILS_PATH
            )
        )
        RuntimeDatabaseHelper.text_search_index = (
            TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
        )
        log.info("Load tables done.")

    @staticmethod
    @abstractmethod
    def has_vacuum_tablename() -> bool:
        pass

    @staticmethod
    @abstractmethod
    def is_vacuum_full() -> bool:
        pass

    @staticmethod
    @abstractmethod
    def is_vacuum_analyze() -> bool:
        pass

    @staticmethod
    @abstractmethod
    def generate_insert_query(
        schema_class: Type[RuntimeConcreteTable],
        values: dict,
        on_conflict_do_nothing=False,
    ) -> ReturningInsert[tuple[RuntimeConcreteTable]]:
        pass

    @staticmethod
    @abstractmethod
    def generate_insert_bulk_query(
        schema_class: Type[RuntimeConcreteTable],
        values: List[dict],
        on_conflict_do_nothing=False,
    ) -> Insert[tuple[RuntimeConcreteTable]]:
        pass

    @staticmethod
    def get_network(
        entity_repository: RuntimeEntityRepository,
        relation_repository: RuntimeRelationRepository,
        entity_id: int,
        entity_type: EntityType,
        on_mobile=False,
        roles=None,
    ):
        from discograph.runtime.data_access_layer.relation_grapher import (
            RelationGrapher,
        )

        cache = CacheManager.get_cache()

        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        template = "discograph:/api/{entity_type}/network/{entity_id}"
        if on_mobile:
            template += "/mobile"

        cache_key = RelationGrapher.make_cache_key(
            template,
            entity_id,
            entity_type,
            roles=roles,
        )
        # cache_key = cache_key.format(entity_type, entity_id)
        log.debug(f"  get cache_key: {cache_key}")
        data = cache.get(cache_key)
        if data is not None:
            return data

        try:
            entity = entity_repository.get_by_entity_id_and_entity_type(
                entity_id, entity_type
            )
        except NotFoundError:
            return None
        if entity is None:
            return None
        if not on_mobile:
            max_nodes = RuntimeDatabaseHelper.MAX_NODES
            degree = RuntimeDatabaseHelper.MAX_DEGREE
        else:
            max_nodes = RuntimeDatabaseHelper.MAX_NODES_MOBILE
            degree = RuntimeDatabaseHelper.MAX_DEGREE_MOBILE
        relation_grapher = RelationGrapher(
            center_entity=entity,
            degree=degree,
            max_nodes=max_nodes,
            role_names=roles,
        )
        data = relation_grapher.get_relation_graph(
            entity_repository, relation_repository
        )
        cache.set(cache_key, data)
        return data

    @staticmethod
    def get_random_entity(
        entity_repository: RuntimeEntityRepository,
    ) -> tuple[int, EntityType]:

        # structural_roles = [
        #     "Alias",
        #     "Member Of",
        #     "Sublabel Of",
        # ]
        # if role_names and any(_ not in structural_roles for _ in role_names):
        #     relation = relation_repository.get_random(role_names=role_names)
        #     entity_choice = random.randint(1, 2)
        #     if entity_choice == 1:
        #         entity_type = relation.entity_one_type
        #         entity_id = relation.entity_one_id
        #     else:
        #         entity_type = relation.entity_two_type
        #         entity_id = relation.entity_two_id
        #     log.debug("random link")
        # else:
        counter = 0

        while True:
            random_id = RuntimeDatabaseHelper.search_get_random_id()
            entity_id, entity_type = to_entity_external_id(random_id)
            if entity_type == EntityType.LABEL:
                log.debug("random skip label")
                entity = None
                continue
            try:
                entity = entity_repository.get_by_id(random_id)
            except NotFoundError:
                log.debug("random not found")
                counter += 1
                entity = None
                continue

            # if DatabaseHelper.entity_count_cached == 0:
            #     DatabaseHelper.entity_count_cached = entity_repository.count()
            # random_id = random.randint(1, DatabaseHelper.entity_count_cached)
            # try:
            #     entity = entity_repository.get_random_by_id(random_id)
            #     # entity = entity_repository.get_by_id(random_id)
            # except NotFoundError:
            #     counter += 1
            #     entity = None
            #     continue

            relation_counts = entity.relation_counts
            entities = entity.entities
            # log.debug(f"relation_counts: {relation_counts}")
            counter += 1
            if entity.entity_type == EntityType.LABEL:
                log.debug("random skip label")
                continue
            if (
                relation_counts is not None
                and (
                    "Member Of" in relation_counts
                    or "Alias" in relation_counts
                    or ("members" in entities and len(entities["members"]) > 0)
                    or ("groups" in entities and len(entities["groups"]) > 0)
                )
                and entity.entity_type == EntityType.ARTIST
            ):
                log.debug(f"random node: {entity} counter: {counter}")
                break
            else:
                log.debug(f"random fail: {entity} counter: {counter}")

            if counter >= 1000:
                log.debug("random count expired")
                break

        if entity:
            entity_id, entity_type = entity.entity_id, entity.entity_type
        else:
            entity_id = 0
            entity_type = EntityType.ARTIST

        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        return entity_id, entity_type

    @classmethod
    def get_relations_by_entity_id_and_entity_type(
        cls,
        entity_repository: RuntimeEntityRepository,
        relation_repository: RuntimeRelationRepository,
        # relation_release_year_repository: RuntimeRelationReleaseYearRepository,
        entity_id: int,
        entity_type: EntityType,
    ) -> dict[str, Any]:
        # TODO Add info on releases back in one day
        entity = entity_repository.get_by_entity_id_and_entity_type(
            entity_id, entity_type
        )
        relations = relation_repository.find_by_entity(entity.id)

        data = []
        for relation in relations:
            # relation_release_years = relation_release_year_repository.get(relation.id)
            relation_releases = {}
            # for relation_release_year in relation_release_years:
            #     relation_releases[relation_release_year.release_id] = (
            #         relation_release_year.year
            #     )

            # category = RoleType.role_definitions[relation.role]
            # if category is None:
            #     continue
            datum = {
                "role": relation.role,
                "releases": relation_releases,
            }
            data.append(datum)
        data = {"results": tuple(data)}
        return data

    # @classmethod
    # def get_relation_by_key(
    #     cls,
    #     relation_repository: RuntimeRelationRepository,
    #     relation_release_year_repository: RuntimeRelationReleaseYearRepository,
    #     key: dict[str, Any],
    # ) -> RuntimeRelation:
    #     relation_internal = relation_repository.find_by_key(key)
    #     relation = relation_internal.to_relation()
    #
    #     relation_release_years = relation_release_year_repository.get(relation.id)
    #     relation.releases = {}
    #     for relation_release_year in relation_release_years:
    #         relation.releases[str(relation_release_year.release_id)] = (
    #             relation_release_year.year
    #         )
    #     return relation

    @classmethod
    def search_text_index(cls, search_text):
        return RuntimeDatabaseHelper.text_search_index.search(search_text)

    @classmethod
    def search_get_random_id(cls):
        return RuntimeDatabaseHelper.text_search_index.get_random_id()
