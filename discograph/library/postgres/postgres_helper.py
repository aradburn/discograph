import logging
import os
import pathlib
import shutil
from typing import Type, List

from pg_temp import TempDB
from sqlalchemy import Engine, URL, create_engine, text
from sqlalchemy.dialects.postgresql import insert, Insert
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
                    "work_mem": "300MB",
                    "maintenance_work_mem": "300MB",
                    "effective_cache_size": "2GB",
                    "max_connections": get_concurrency_count() + 4,
                    "shared_buffers": "3GB",
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
                    # connect_args={
                    #     "connect_timeout": 10,
                    # },
                    # isolation_level="REPEATABLE READ",
                    # isolation_level="SERIALIZABLE",
                    # execution_options={
                    #     # "isolation_level": "REPEATABLE READ",
                    #     "statement_timeout": 20000,
                    #     "lock_timeout": 10000,
                    #     "idle_in_transaction_session_timeout": 30000,
                    # },
                    # poolclass=NullPool,
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
                    pool_size=get_concurrency_count(),
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
    def shutdown_database() -> None:
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
    def check_connection(config: Configuration, engine: Engine) -> None:
        try:
            log.info("Check Postgres database connection...")

            with engine.connect() as connection:
                version = connection.execute(text("SELECT version();"))
                connection.commit()

            log.info(f"Database Version: {version.scalars().one_or_none()}")

            log.info("Database connected OK.")
        except DatabaseError:
            log.exception("Connection Error", exc_info=True)

    @classmethod
    def create_tables(cls, tables: List[str] = None) -> None:
        log.info("Create Postgres tables")
        super().create_tables(tables=tables)

    @classmethod
    def drop_tables(cls) -> None:
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
