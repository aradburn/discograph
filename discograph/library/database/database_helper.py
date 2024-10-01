import logging
import random
from abc import ABC, abstractmethod
from functools import partial
from typing import Type, List, Any

from sqlalchemy import Engine, Index, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql.dml import ReturningInsert, Insert

from discograph.config import Configuration
from discograph.library.database.base_table import Base, ConcreteTable
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.relation_release_year_repository import (
    RelationReleaseYearRepository,
)
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.domain.relation import Relation
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


# class Base(DeclarativeBase):
#     pass
#
#
# ConcreteTable = TypeVar("ConcreteTable", bound=Base)


class DatabaseHelper(ABC):
    engine: Engine | None = None
    session_factory: sessionmaker | None = None
    flask_db_session: scoped_session | None = None

    db_helper: Type["DatabaseHelper"] | None = None
    idx_entity_one_id: Index | None = None
    idx_entity_two_id: Index | None = None

    MAX_NODES = 400
    MAX_NODES_MOBILE = 25

    MAX_DEGREE = 3
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
        from discograph.library.database import ALL_DATABASE_TABLES

        for table in ALL_DATABASE_TABLES:
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
        from discograph.library.loader.loader_entity import LoaderEntity
        from discograph.library.loader.loader_relation import LoaderRelation
        from discograph.library.loader.loader_release import LoaderRelease
        from discograph.library.loader.loader_role import LoaderRole

        has_tablename = cls.has_vacuum_tablename()
        is_full = cls.is_vacuum_full()
        is_analyze = cls.is_vacuum_analyze()
        stages = [
            partial(LoaderRole().load_all_roles),
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

    @staticmethod
    def get_network(
        entity_repository: EntityRepository,
        relation_repository: RelationRepository,
        entity_id: int,
        entity_type: EntityType,
        on_mobile=False,
        roles=None,
    ):
        from discograph.library.cache.cache_manager import cache
        from discograph.library.relation_grapher import RelationGrapher

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
        # log.debug(f"  get cache_key: {cache_key}")
        data = cache.get(cache_key)
        if data is not None:
            return data

        entity = entity_repository.get(entity_id, entity_type)
        if entity is None:
            return None
        if not on_mobile:
            max_nodes = DatabaseHelper.MAX_NODES
            degree = DatabaseHelper.MAX_DEGREE
        else:
            max_nodes = DatabaseHelper.MAX_NODES_MOBILE
            degree = DatabaseHelper.MAX_DEGREE_MOBILE
        relation_grapher = RelationGrapher(
            center_entity=entity,
            degree=degree,
            max_nodes=max_nodes,
            role_names=roles,
        )
        data = relation_grapher.get_relation_graph(relation_repository)
        cache.set(cache_key, data)
        return data

    @staticmethod
    def get_random_entity(
        entity_repository: EntityRepository,
        relation_repository: RelationRepository,
        role_names: List[str] = None,
    ) -> tuple[int, EntityType]:

        structural_roles = [
            "Alias",
            "Member Of",
            "Sublabel Of",
        ]
        if role_names and any(_ not in structural_roles for _ in role_names):
            relation = relation_repository.get_random(role_names=role_names)
            entity_choice = random.randint(1, 2)
            if entity_choice == 1:
                entity_type = relation.entity_one_type
                entity_id = relation.entity_one_id
            else:
                entity_type = relation.entity_two_type
                entity_id = relation.entity_two_id
            log.debug("random link")
        else:
            counter = 0

            while True:
                entity = entity_repository.get_random()
                relation_counts = entity.relation_counts
                entities = entity.entities
                # log.debug(f"relation_counts: {relation_counts}")
                counter = counter + 1
                if entity.entity_type == EntityType.LABEL:
                    log.debug("random skip label")
                    continue
                if (
                    relation_counts is not None
                    and (
                        "Member Of" in relation_counts
                        or "Alias" in relation_counts
                        or "members" in entities
                    )
                    and entity.entity_type == EntityType.ARTIST
                ):
                    log.debug(f"random node: {entity}")
                    break
                else:
                    log.debug(f"random fail: {entity}")

                if counter >= 1000:
                    log.debug("random count expired")
                    break

            entity_id, entity_type = entity.entity_id, entity.entity_type
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        return entity_id, entity_type

    @staticmethod
    def search_entities(entity_repository: EntityRepository, search_string):
        from discograph.utils import URLIFY_REGEX
        from discograph.library.cache.cache_manager import cache

        search_query_url = URLIFY_REGEX.sub("+", search_string)
        cache_key = f"discograph:/api/search/{search_query_url}"
        log.debug(f"  get cache_key: {cache_key}")
        data = cache.get(cache_key)
        if data is not None:
            log.debug(f"{cache_key}: CACHED")
            for datum in data["results"]:
                log.debug(f"    {datum}")
            return data

        entities = entity_repository.find_by_search_content(search_string)
        log.debug(f"{cache_key}: NOT CACHED")
        data = []
        for entity in entities:
            datum = dict(
                key=entity.json_entity_key,
                name=entity.entity_name,
            )
            data.append(datum)
            log.debug(f"    {datum}")
        data = {"results": tuple(data)}
        log.debug(f"  set cache_key: {cache_key} data: {data}")
        cache.set(cache_key, data)
        return data

    @classmethod
    def get_relations_by_entity_id_and_entity_type(
        cls,
        relation_repository: RelationRepository,
        relation_release_year_repository: RelationReleaseYearRepository,
        entity_id: int,
        entity_type: EntityType,
    ) -> dict[str, Any]:
        relations = relation_repository.find_by_entity_id_and_entity_type(
            entity_id, entity_type
        )

        data = []
        for relation in relations:
            relation_release_years = relation_release_year_repository.get(
                relation.relation_id
            )
            relation.releases = {}
            for relation_release_year in relation_release_years:
                relation.releases[relation_release_year.release_id] = (
                    relation_release_year.year
                )

            # category = RoleType.role_definitions[relation.role]
            # if category is None:
            #     continue
            datum = {
                "role": relation.role,
                "releases": relation.releases,
            }
            data.append(datum)
        data = {"results": tuple(data)}
        return data

    @classmethod
    def get_relation_by_key(
        cls,
        relation_repository: RelationRepository,
        relation_release_year_repository: RelationReleaseYearRepository,
        key: dict[str, Any],
    ) -> Relation:
        relation = relation_repository.find_by_key(key)

        relation_release_years = relation_release_year_repository.get(
            relation.relation_id
        )
        relation.releases = {}
        for relation_release_year in relation_release_years:
            relation.releases[str(relation_release_year.release_id)] = (
                relation_release_year.year
            )
        return relation
