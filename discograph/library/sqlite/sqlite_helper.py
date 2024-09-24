import logging
import pathlib
from typing import Type, List

from sqlalchemy import Engine, create_engine, text, StaticPool
from sqlalchemy.dialects.sqlite import insert, Insert
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.dml import ReturningInsert

from discograph.config import Configuration
from discograph.library.database.database_helper import DatabaseHelper, ConcreteTable

log = logging.getLogger(__name__)


class SqliteHelper(DatabaseHelper):

    @staticmethod
    def setup_database(config: Configuration) -> Engine:
        log.info("Using Sqlite Database")
        # if config["TESTING"]:
        #     engine = create_engine(
        #         "sqlite://",
        #         connect_args={
        #             "check_same_thread": False,
        #         },
        #         poolclass=StaticPool,
        #     )
        # else:
        target_path = pathlib.Path(config["SQLITE_DATABASE_NAME"])
        target_parent = target_path.parent
        target_parent.mkdir(parents=True, exist_ok=True)
        log.info(f"Sqlite Database: {target_path}")

        engine = create_engine(
            f"sqlite:///{target_path}",
            connect_args={
                "check_same_thread": False,
            },
            poolclass=StaticPool,
        )
        return engine

    @staticmethod
    def shutdown_database() -> None:
        log.info("Shutting down Sqlite database")

    @staticmethod
    def check_connection(config: Configuration, engine: Engine) -> None:
        try:
            log.info("Check Sqlite database connection...")

            with engine.connect() as connection:
                version = connection.execute(
                    text("SELECT sqlite_version() AS version;")
                )
                log.info(f"Database Version: {version.scalars().one_or_none()}")

                connection.execute(text("pragma journal_mode=MEMORY"))
                # connection.execute(text("pragma journal_mode=wal"))
                connection.execute(text("pragma synchronous=OFF"))
                connection.execute(text("pragma cache_size=-10000"))
                connection.execute(text("pragma temp_store=MEMORY"))
                connection.execute(text("pragma foreign_keys=ON"))
                connection.commit()
                log.info("Database connected OK.")
        except DatabaseError:
            log.exception("Connection Error", exc_info=True)

    @classmethod
    def load_tables(cls, data_directory: str, date: str, is_bulk_inserts: bool) -> None:
        log.info("Load Sqlite tables")
        stages = cls.get_load_table_stages(data_directory, date, is_bulk_inserts)
        for stage in stages:
            stage()

        log.info("Load Sqlite done.")

    @classmethod
    def load_table_stage(
        cls, data_directory: str, date: str, is_bulk_inserts: bool, stage: int
    ) -> None:
        stages = cls.get_load_table_stages(data_directory, date, is_bulk_inserts)
        log.debug(f"Run stage: {stage}")
        stages[stage]()

    @classmethod
    def create_tables(cls, tables: List[str] = None) -> None:
        log.info("Create Sqlite tables")
        super().create_tables(tables=tables)

    @classmethod
    def drop_tables(cls, tables: List[str] = None) -> None:
        log.info("Drop Sqlite tables")
        super().drop_tables(tables=tables)

    @staticmethod
    def has_vacuum_tablename() -> bool:
        return False

    @staticmethod
    def is_vacuum_full() -> bool:
        return False

    @staticmethod
    def is_vacuum_analyze() -> bool:
        return False

    @staticmethod
    def generate_insert_query(
        schema_class: Type[ConcreteTable], values: dict, on_conflict_do_nothing=False
    ) -> ReturningInsert[tuple[ConcreteTable]]:
        if on_conflict_do_nothing:
            return (
                insert(schema_class)
                .on_conflict_do_nothing()
                .values(values)
                .returning(schema_class)
            )
        else:
            return insert(schema_class).values(values).returning(schema_class)

    @staticmethod
    def generate_insert_bulk_query(
        schema_class: Type[ConcreteTable],
        values_list: List[dict],
        on_conflict_do_nothing=False,
    ) -> Insert[tuple[ConcreteTable]]:
        if on_conflict_do_nothing:
            return insert(schema_class).on_conflict_do_nothing().values(values_list)
        else:
            return insert(schema_class).values(values_list)

    # @staticmethod
    # def get_entity(entity_type: EntityType, entity_id: int):
    #     where_clause = SqliteEntity.entity_id == entity_id
    #     where_clause &= SqliteEntity.entity_type == entity_type
    #     with LoaderBase.connection_context():
    #         query = SqliteEntity.select().where(where_clause)
    #         return query.get_or_none()

    # @staticmethod
    # def get_network(
    #     session: Session,
    #     entity_id: int,
    #     entity_type: EntityType,
    #     on_mobile=False,
    #     roles=None,
    # ):
    #     from discograph.library.cache.cache_manager import cache
    #
    #     log.debug(f"entity_type: {entity_type}")
    #     assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
    #     template = "discograph:/api/{entity_type}/network/{entity_id}"
    #     if on_mobile:
    #         template += "/mobile"
    #
    #     cache_key_formatter = SqliteRelationGrapher.make_cache_key(
    #         template,
    #         entity_id,
    #         entity_type,
    #         roles=roles,
    #     )
    #     cache_key = cache_key_formatter.format(entity_type, entity_id)
    #     log.debug(f"cache_key: {cache_key}")
    #     data = cache.get(cache_key)
    #     if data is not None:
    #         return data
    #     # entity_type = entity_name_types[entity_type]
    #     entity = SqliteHelper.get_entity(session, entity_id, entity_type)
    #     log.debug(f"entity: {entity}")
    #     if entity is None:
    #         return None
    #     if not on_mobile:
    #         max_nodes = DatabaseHelper.MAX_NODES
    #         degree = DatabaseHelper.MAX_DEGREE
    #     else:
    #         max_nodes = DatabaseHelper.MAX_NODES_MOBILE
    #         degree = DatabaseHelper.MAX_DEGREE_MOBILE
    #     relation_grapher = SqliteRelationGrapher(
    #         center_entity=entity,
    #         degree=degree,
    #         max_nodes=max_nodes,
    #         roles=roles,
    #     )
    #     data = relation_grapher.get_relation_graph(session)
    #     cache.set(cache_key, data)
    #     return data
    #
    # @staticmethod
    # def get_random_entity(session: Session, roles=None) -> tuple[int, str]:
    #     structural_roles = [
    #         "Alias",
    #         "Member Of",
    #         "Sublabel Of",
    #     ]
    #     if roles and any(_ not in structural_roles for _ in roles):
    #         relation = SqliteRelationDB.get_random_relation(session, roles=roles)
    #         entity_choice = random.randint(1, 2)
    #         if entity_choice == 1:
    #             entity_type = relation.entity_one_type
    #             entity_id = relation.entity_one_id
    #         else:
    #             entity_type = relation.entity_two_type
    #             entity_id = relation.entity_two_id
    #         log.debug("random link")
    #     else:
    #         counter = 0
    #
    #         while True:
    #             entity: SqliteEntityDB = SqliteEntityDB.get_random_entity(session)
    #             relation_counts = entity.relation_counts
    #             entities = entity.entities
    #             # log.debug(f"relation_counts: {relation_counts}")
    #             counter = counter + 1
    #             if (
    #                 relation_counts is not None
    #                 and (
    #                     "Member Of" in relation_counts
    #                     or "Alias" in relation_counts
    #                     or "members" in entities
    #                 )
    #                 and entity.entity_type == EntityType.ARTIST
    #             ):
    #                 log.debug(f"random node: {entity}")
    #                 break
    #             if entity.entity_type == EntityType.LABEL:
    #                 log.debug("random skip label")
    #                 continue
    #             if counter >= 1000:
    #                 log.debug("random count expired")
    #                 break
    #
    #         entity_id, entity_type = entity.entity_id, entity.entity_type
    #     assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
    #     return entity_id, entity_type
    #
    # @staticmethod
    # def get_relations(session: Session, entity_id: int, entity_type: EntityType):
    #     entity = SqliteHelper.get_entity(session, entity_id, entity_type)
    #     if entity is None:
    #         return None
    #     where_clause = SqliteRelationDB.search(
    #         entity_id=entity.entity_id,
    #         entity_type=entity.entity_type,
    #         query_only=True,
    #     )
    #     relations = session.scalars(
    #         select(SqliteRelationDB)
    #         .where(where_clause)
    #         .order_by(
    #             SqliteRelationDB.role,
    #             SqliteRelationDB.entity_one_id,
    #             SqliteRelationDB.entity_one_type,
    #             SqliteRelationDB.entity_two_id,
    #             SqliteRelationDB.entity_two_type,
    #         )
    #     )
    #     data = []
    #     for relation in relations:
    #         category = RoleType.role_definitions[relation.role]
    #         if category is None:
    #             continue
    #         datum = {
    #             "role": relation.role,
    #         }
    #         data.append(datum)
    #     data = {"results": tuple(data)}
    #     return data
    #
    # @staticmethod
    # def parse_request_args(args):
    #     from discograph.utils import ARG_ROLES_REGEX
    #
    #     year = None
    #     roles = set()
    #     for key in args:
    #         if key == "year":
    #             year = args[key]
    #             try:
    #                 if "-" in year:
    #                     start, _, stop = year.partition("-")
    #                     year = tuple(sorted((int(start), int(stop))))
    #                 else:
    #                     year = int(year)
    #             finally:
    #                 pass
    #         elif ARG_ROLES_REGEX.match(key):
    #             value = args.getlist(key)
    #             for role in value:
    #                 if role in RoleType.role_definitions:
    #                     roles.add(role)
    #     roles = list(sorted(roles))
    #     return roles, year
    #
    # @staticmethod
    # def search_entities(session: Session, search_string):
    #     from discograph.utils import URLIFY_REGEX
    #     from discograph.library.cache.cache_manager import cache
    #
    #     cache_key = f"discograph:/api/search/{URLIFY_REGEX.sub('+', search_string)}"
    #     log.debug(f"  get cache_key: {cache_key}")
    #     data = cache.get(cache_key)
    #     if data is not None:
    #         log.debug(f"{cache_key}: CACHED")
    #         for datum in data["results"]:
    #             log.debug(f"    {datum}")
    #         return data
    #     select_query = SqliteEntityDB.build_search_text_query(search_string)
    #     log.debug(f"select_query: {select_query}")
    #     entities = session.scalars(select(EntityDB).filter(select_query)).all()
    #     log.debug(f"{cache_key}: NOT CACHED")
    #     data = []
    #     for entity in entities:
    #         datum = dict(
    #             key=entity.json_entity_key,
    #             name=entity.entity_name,
    #         )
    #         data.append(datum)
    #         log.debug(f"    {datum}")
    #     data = {"results": tuple(data)}
    #     log.debug(f"  set cache_key: {cache_key} data: {data}")
    #     cache.set(cache_key, data)
    #     return data
