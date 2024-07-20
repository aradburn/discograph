import logging
import os
import pathlib
import shutil
from typing import Type

from pg_temp import TempDB
from sqlalchemy import Engine, URL, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.dml import ReturningInsert

from discograph.config import Configuration
from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper, ConcreteTable

log = logging.getLogger(__name__)


class PostgresHelper(DatabaseHelper):
    postgres_test_db: TempDB = None
    _is_test: bool = False

    @staticmethod
    def setup_database(config: Configuration) -> Engine:
        if config["PRODUCTION"]:
            log.info("**************************************")
            log.info("* Using Production Postgres Database *")
            log.info("**************************************")
            log.info("")

            # Create a database engine and pool that will manage connections and execute queries
            url_object = URL.create(
                "postgresql+psycopg2",
                username=config["POSTGRES_DATABASE_USERNAME"],
                password=config["POSTGRES_DATABASE_PASSWORD"],
                host=config["POSTGRES_DATABASE_HOST"],
                port=config["POSTGRES_DATABASE_PORT"],
                database=config["POSTGRES_DATABASE_NAME"],
            )
            engine = create_engine(
                url_object, pool_size=40, pool_timeout=300, pool_recycle=300
            )

            # with database.connection_context():
            # database.execute_sql("SET auto_explain.log_analyze TO on;")
            # database.execute_sql("SET auto_explain.log_min_duration TO 500;")
            # database.execute_sql("CREATE EXTENSION pg_stat_statements;")

        else:
            if config["TESTING"]:
                log.info("Using Postgres Test Database")

                dirname = config["POSTGRES_DATA"]
                pg_data_dir = os.path.join(dirname, "data")
                pg_socket_dir = os.path.join(dirname, "socket")

                data_path = pathlib.Path(pg_data_dir)
                socket_path = pathlib.Path(pg_socket_dir)
                log.debug(f"data_path: {data_path}")
                log.debug(f"socket_path: {socket_path}")

                # pg_data_path = pathlib.Path(pg_data_dir)
                # if config['TESTING']:
                #     data_path.rmdir()
                data_path.parent.mkdir(parents=True, exist_ok=True)

                # Delete left over failed test database if present
                if data_path.is_dir():
                    shutil.rmtree(data_path)
                if socket_path.is_dir():
                    shutil.rmtree(socket_path)

                options = {
                    "work_mem": "500MB",
                    "maintenance_work_mem": "500MB",
                    "effective_cache_size": "2GB",
                    "max_connections": get_concurrency_count() * 2,
                    "shared_buffers": "2GB",
                    # "log_min_duration_statement": 5000,
                    # "shared_preload_libraries": 'pg_stat_statements',
                    # "session_preload_libraries": 'auto_explain',
                    # "default_transaction_isolation": "serializable",
                    # "transaction_isolation": "serializable",
                    # "statement_timeout": "20000",
                    # "lock_timeout": "10000",
                    # "idle_in_transaction_session_timeout": "30000",
                }
                PostgresHelper.postgres_test_db = TempDB(
                    verbosity=0,
                    databases=[config["POSTGRES_DATABASE_NAME"]],
                    initdb=config["POSTGRES_ROOT"] + "/bin/initdb",
                    postgres=config["POSTGRES_ROOT"] + "/bin/postgres",
                    psql=config["POSTGRES_ROOT"] + "/bin/psql",
                    createuser=config["POSTGRES_ROOT"] + "/bin/createuser",
                    dirname=dirname,
                    options=options,
                )

                # Create a temporary test database engine and pool that will manage connections and execute queries
                url_object = URL.create(
                    "postgresql",
                    # "postgresql+psycopg2",
                    username=PostgresHelper.postgres_test_db.current_user,
                    # password=config["POSTGRES_DATABASE_PASSWORD"],
                    host=PostgresHelper.postgres_test_db.pg_socket_dir,
                    # port=config["POSTGRES_DATABASE_PORT"],
                    database=config["POSTGRES_DATABASE_NAME"],
                )
                engine = create_engine(
                    url_object,
                    pool_size=get_concurrency_count(),
                    pool_timeout=30,
                    pool_recycle=30,
                    connect_args={
                        "connect_timeout": 10,
                    },
                    # isolation_level="REPEATABLE READ",
                    # isolation_level="SERIALIZABLE",
                    # execution_options={
                    #     # "isolation_level": "REPEATABLE READ",
                    #     "statement_timeout": 20000,
                    #     "lock_timeout": 10000,
                    #     "idle_in_transaction_session_timeout": 30000,
                    # },
                )

                PostgresHelper._is_test = True
            else:
                log.info("Using Postgres Development Database")

                # Create a database engine and pool that will manage connections and execute queries
                url_object = URL.create(
                    "postgresql+psycopg2",
                    username=config["POSTGRES_DATABASE_USERNAME"],
                    password=config["POSTGRES_DATABASE_PASSWORD"],
                    host=config["POSTGRES_DATABASE_HOST"],
                    port=config["POSTGRES_DATABASE_PORT"],
                    database=config["POSTGRES_DATABASE_NAME"],
                )
                engine = create_engine(
                    url_object,
                    pool_size=40,
                    pool_timeout=300,
                    pool_recycle=300,
                    connect_args={
                        "connect_timeout": 1000,
                    },
                    # isolation_level="REPEATABLE READ",
                    # isolation_level="SERIALIZABLE",
                    # execution_options={
                    #     # "isolation_level": "REPEATABLE READ",
                    #     "statement_timeout": 20000,
                    #     "lock_timeout": 10000,
                    #     "idle_in_transaction_session_timeout": 30000,
                    # },
                )

        return engine

    @staticmethod
    def shutdown_database():
        log.info("Shutting down Postgres database")

        if PostgresHelper._is_test and PostgresHelper.postgres_test_db is not None:
            log.info("Cleaning up Postgres Test Database")
            PostgresHelper.postgres_test_db.cleanup()

            log.info(f"Delete data dir: {PostgresHelper.postgres_test_db.pg_data_dir}")
            shutil.rmtree(PostgresHelper.postgres_test_db.pg_data_dir)
            log.info(
                f"Delete socket dir: {PostgresHelper.postgres_test_db.pg_socket_dir}"
            )
            shutil.rmtree(PostgresHelper.postgres_test_db.pg_socket_dir)
            PostgresHelper.postgres_test_db = None
            PostgresHelper._is_test = False

    @staticmethod
    def check_connection(config: Configuration, engine: Engine):
        try:
            log.info("Check Postgres database connection...")

            with engine.connect() as connection:
                version = connection.execute(text("SELECT version();"))

            log.info(f"Database Version: {version.scalars().one_or_none()}")

            log.info("Database connected OK.")
        except DatabaseError as e:
            log.exception("Connection Error", e)

    @classmethod
    def create_tables(cls, tables=None):
        log.info("Create Postgres tables")
        super().create_tables(tables=tables)

    @classmethod
    def drop_tables(cls):
        log.info("Drop Postgres tables")
        super().drop_tables()

    @staticmethod
    def has_vacuum_tablename() -> bool:
        return True

    @staticmethod
    def is_vacuum_full() -> bool:
        return True

    @staticmethod
    def is_vacuum_analyze() -> bool:
        return True

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

    # @classmethod
    # def get_entity(cls, session: Session, entity_id: int, entity_type: EntityType):
    #     pk = (entity_id, entity_type)
    #     return session.get(cls, pk)

    # @staticmethod
    # def get_entity(entity_type: EntityType, entity_id: int):
    #     where_clause = PostgresEntity.entity_id == entity_id
    #     where_clause &= PostgresEntity.entity_type == entity_type
    #     with LoaderBase.connection_context():
    #         query = PostgresEntity.select().where(where_clause)
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
    #     assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
    #     template = "discograph:/api/{entity_type}/network/{entity_id}"
    #     if on_mobile:
    #         template += "/mobile"
    #
    #     cache_key = PostgresRelationGrapher.make_cache_key(
    #         template,
    #         entity_id,
    #         entity_type,
    #         roles=roles,
    #     )
    #     cache_key = cache_key.format(entity_type, entity_id)
    #     log.debug(f"  get cache_key: {cache_key}")
    #     data = cache.get(cache_key)
    #     if data is not None:
    #         return data
    #     # entity_type = entity_name_types[entity_type]
    #     entity = PostgresHelper.get_entity(session, entity_id, entity_type)
    #     if entity is None:
    #         return None
    #     if not on_mobile:
    #         max_nodes = DatabaseHelper.MAX_NODES
    #         degree = DatabaseHelper.MAX_DEGREE
    #     else:
    #         max_nodes = DatabaseHelper.MAX_NODES_MOBILE
    #         degree = DatabaseHelper.MAX_DEGREE_MOBILE
    #     relation_grapher = PostgresRelationGrapher(
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
    #         relation = PostgresRelationDB.get_random_relation(session, roles=roles)
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
    #             entity: PostgresEntityDB = PostgresEntityDB.get_random_entity(session)
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
    #     entity = PostgresHelper.get_entity(session, entity_id, entity_type)
    #     if entity is None:
    #         return None
    #     where_clause = PostgresRelationDB.search(
    #         entity_id=entity.entity_id,
    #         entity_type=entity.entity_type,
    #         query_only=True,
    #     )
    #     relations = session.scalars(
    #         select(PostgresRelationDB)
    #         .where(where_clause)
    #         .order_by(
    #             PostgresRelationDB.role_id,
    #             PostgresRelationDB.entity_one_id,
    #             PostgresRelationDB.entity_one_type,
    #             PostgresRelationDB.entity_two_id,
    #             PostgresRelationDB.entity_two_type,
    #         )
    #     ).all()
    #     data = []
    #     for relation in relations:
    #         # category = RoleType.role_definitions[relation.role]
    #         # if category is None:
    #         #     continue
    #         datum = {
    #             "role": RoleType.role_id_to_role_lookup[relation.role_id],
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
    #     select_query = PostgresEntityDB.build_search_text_query(search_string)
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

    # @classmethod
    # def build_search_text_query(cls, search_string) -> ColumnElement[bool]:
    #     return cls.search_content.match(search_string)
    #     # search_string = search_string.lower()
    #     # # Transliterate the unicode string into a plain ASCII string
    #     # search_string = unidecode(search_string, "preserve")
    #     # search_string = ",".join(search_string.split())
    #     # # TODO fix search_string injection
    #     # query = f"""
    #     #     SELECT entity_type,
    #     #         entity_id,
    #     #         name,
    #     #         ts_rank_cd(search_content, query, 63) AS rank
    #     #     FROM postgresentity,
    #     #         to_tsquery({search_string}) query
    #     #     WHERE query @@ search_content
    #     #     ORDER BY rank DESC
    #     #     LIMIT 100
    #     #     """
    #     # return query
