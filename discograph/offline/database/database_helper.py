import logging
from abc import ABC, abstractmethod
from functools import partial
from typing import Type, List, Any

from sqlalchemy import Engine, Index, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.dml import ReturningInsert, Insert

from discograph.config import Configuration, ENTITY_DETAILS_PATH, TEXT_SEARCH_PATH
from discograph.library.full_text_search.entity_details_index import EntityDetailsIndex
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.offline.data_access_layer.role_data_access import RoleDataAccess
from discograph.offline.database.base_table import Base, ConcreteTable
from discograph.offline.database.relation_release_year_repository import (
    RelationReleaseYearRepository,
)
from discograph.offline.database.relation_repository import RelationRepository
from discograph.offline.domain.relation import Relation

log = logging.getLogger(__name__)


class DatabaseHelper(ABC):
    engine: Engine | None = None
    session_factory: sessionmaker | None = None
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
        cls.engine.dispose(close=False)
        # cls.session_factory = sessionmaker(bind=cls.engine)

    @staticmethod
    @abstractmethod
    def check_connection(config: Configuration, engine: Engine) -> None:
        pass

    @classmethod
    @abstractmethod
    def create_tables(cls, tables: List[str] = None) -> None:
        from discograph.offline.database import ALL_OFFLINE_DATABASE_TABLES

        for table in ALL_OFFLINE_DATABASE_TABLES:
            log.debug(f"table definition for: {table.__tablename__}")
        for table in Base.metadata.tables:
            log.debug(f"table in metadata: {table}")
        table_definitions: List[Table] = [
            Base.metadata.tables[table_name] for table_name in tables
        ]
        for table in table_definitions:
            log.debug(f"creating table: {table.name}")
        Base.metadata.create_all(cls.engine, checkfirst=True, tables=table_definitions)

    @classmethod
    @abstractmethod
    def drop_tables(cls, tables: List[str] = None) -> None:
        if tables is not None:
            table_definitions: List[Table] = [
                Base.metadata.tables[table_name] for table_name in tables
            ]
            for table in table_definitions:
                log.debug(f"deleting table: {table.name}")
                table.drop(cls.engine, checkfirst=True)
        else:
            Base.metadata.drop_all(cls.engine, checkfirst=True)

    @classmethod
    def load_tables(cls, data_directory: str, date: str, is_bulk_inserts: bool) -> None:
        log.info("Load tables")
        stages = cls.get_load_table_stages(data_directory, date, is_bulk_inserts)
        for stage in stages:
            stage()
        log.info("Load tables done.")

    @classmethod
    def load_table_stage(
        cls, data_directory: str, date: str, is_bulk_inserts: bool, stage: int
    ) -> None:
        stages = cls.get_load_table_stages(data_directory, date, is_bulk_inserts)
        log.debug(f"Run stage: {stage}")
        stages[stage]()

    @classmethod
    def get_load_table_stages(
        cls, data_directory: str, date: str, is_bulk_inserts: bool
    ) -> list[partial]:
        from discograph.offline.loader.loader_entity import LoaderEntity
        from discograph.offline.loader.loader_relation import LoaderRelation
        from discograph.offline.loader.loader_release import LoaderRelease

        has_tablename = cls.has_vacuum_tablename()
        is_full = cls.is_vacuum_full()
        is_analyze = cls.is_vacuum_analyze()
        stages = [
            partial(RoleDataAccess.load_all_roles),
            partial(
                LoaderEntity().loader_entity_pass_one,
                data_directory,
                date,
                is_bulk_inserts,
            ),
            partial(
                LoaderEntity().loader_entity_vacuum, has_tablename, is_full, is_analyze
            ),
            partial(
                LoaderRelease().loader_release_pass_one,
                data_directory,
                date,
                is_bulk_inserts,
            ),
            partial(
                LoaderRelease().loader_release_vacuum,
                has_tablename,
                is_full,
                is_analyze,
            ),
            partial(LoaderEntity().loader_entity_pass_two),
            partial(LoaderRelease().loader_release_pass_two),
            partial(LoaderRelation().loader_relation_pass_one, date),
            # partial(LoaderRelation().loader_relation_pass_two, date),
            partial(
                LoaderEntity().loader_entity_vacuum, has_tablename, is_full, is_analyze
            ),
            partial(
                LoaderRelease().loader_release_vacuum,
                has_tablename,
                is_full,
                is_analyze,
            ),
            partial(
                LoaderRelation().loader_relation_vacuum,
                has_tablename,
                is_full,
                is_analyze,
            ),
            partial(LoaderEntity().loader_entity_pass_three),
            partial(
                LoaderEntity().loader_entity_vacuum, has_tablename, is_full, is_analyze
            ),
            partial(
                LoaderRelease().loader_release_vacuum,
                has_tablename,
                is_full,
                is_analyze,
            ),
            partial(
                LoaderRelation().loader_relation_vacuum,
                has_tablename,
                is_full,
                is_analyze,
            ),
            partial(
                LoaderRelease().loader_create_entity_details_index,
                ENTITY_DETAILS_PATH,
            ),
            partial(
                LoaderEntity().loader_create_text_search_index,
                TEXT_SEARCH_PATH,
            ),
        ]
        return stages

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
        schema_class: Type[ConcreteTable], values: dict, on_conflict_do_nothing=False
    ) -> ReturningInsert[tuple[ConcreteTable]]:
        pass

    @staticmethod
    @abstractmethod
    def generate_insert_bulk_query(
        schema_class: Type[ConcreteTable],
        values: List[dict],
        on_conflict_do_nothing=False,
    ) -> Insert[tuple[ConcreteTable]]:
        pass

    # @classmethod
    # def get_relations_by_entity_id_and_entity_type(
    #     cls,
    #     entity_repository: EntityRepository,
    #     relation_repository: RelationRepository,
    #     relation_release_year_repository: RelationReleaseYearRepository,
    #     entity_id: int,
    #     entity_type: EntityType,
    # ) -> dict[str, Any]:
    #     entity = entity_repository.get_by_entity_id_and_entity_type(
    #         entity_id, entity_type
    #     )
    #     relations = relation_repository.find_by_entity(entity.id)
    #
    #     data = []
    #     for relation in relations:
    #         relation_release_years = relation_release_year_repository.get(relation.id)
    #         relation_releases = {}
    #         for relation_release_year in relation_release_years:
    #             relation_releases[relation_release_year.release_id] = (
    #                 relation_release_year.year
    #             )
    #
    #         # category = RoleType.role_definitions[relation.role]
    #         # if category is None:
    #         #     continue
    #         datum = {
    #             "role": relation.role,
    #             "releases": relation_releases,
    #         }
    #         data.append(datum)
    #     data = {"results": tuple(data)}
    #     return data

    @classmethod
    def get_relation_by_key(
        cls,
        relation_repository: RelationRepository,
        relation_release_year_repository: RelationReleaseYearRepository,
        key: dict[str, Any],
    ) -> Relation:
        relation_internal = relation_repository.find_by_key(key)
        relation = relation_internal.to_relation()

        relation_release_years = relation_release_year_repository.get(relation.id)
        relation.releases = {}
        for relation_release_year in relation_release_years:
            relation.releases[str(relation_release_year.release_id)] = (
                relation_release_year.year
            )
        return relation

    # @classmethod
    # def search_text_index(cls, search_text):
    #     return cls.text_search_index.search(search_text)
    #
    # @classmethod
    # def search_get_random_id(cls):
    #     return cls.text_search_index.get_random_id()
