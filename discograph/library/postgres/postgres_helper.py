import logging
import os
import pathlib
import random
import shutil

import psycopg2
from peewee import Database
from pg_temp import TempDB
from playhouse import pool

from discograph.config import Configuration
from discograph.library.credit_role import CreditRole
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.discogs_model import DiscogsModel
from discograph.library.entity_type import EntityType
from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_relation_grapher import (
    PostgresRelationGrapher,
)

log = logging.getLogger(__name__)


class PostgresHelper(DatabaseHelper):
    postgres_db: TempDB
    _is_test: bool = False

    @staticmethod
    def setup_database(config: Configuration) -> Database:
        if config["PRODUCTION"]:
            log.info("**************************************")
            log.info("* Using Production Postgres Database *")
            log.info("**************************************")
            log.info("")

            # Create a database instance that will manage the connection and execute queries
            database = pool.PooledPostgresqlExtDatabase(
                config["POSTGRES_DATABASE_NAME"],
                host=config["POSTGRES_DATABASE_HOST"],
                port=config["POSTGRES_DATABASE_PORT"],
                user=config["POSTGRES_DATABASE_USERNAME"],
                password=config["POSTGRES_DATABASE_PASSWORD"],
                max_connections=40,
                timeout=300,  # 5 minutes.
                stale_timeout=300,  # 5 minutes.
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

                data_path = pathlib.Path(pg_data_dir)
                # pg_data_path = pathlib.Path(pg_data_dir)
                # if config['TESTING']:
                #     data_path.rmdir()
                data_path.parent.mkdir(parents=True, exist_ok=True)

                options = {
                    "work_mem": "500MB",
                    "maintenance_work_mem": "500MB",
                    "effective_cache_size": "4GB",
                    "max_connections": 34,
                    "shared_buffers": "2GB",
                    # "log_min_duration_statement": 5000,
                    # "shared_preload_libraries": 'pg_stat_statements',
                    # "session_preload_libraries": 'auto_explain',
                }
                PostgresHelper.postgres_db = TempDB(
                    verbosity=0,
                    databases=[config["POSTGRES_DATABASE_NAME"]],
                    initdb=config["POSTGRES_ROOT"] + "/bin/initdb",
                    postgres=config["POSTGRES_ROOT"] + "/bin/postgres",
                    psql=config["POSTGRES_ROOT"] + "/bin/psql",
                    createuser=config["POSTGRES_ROOT"] + "/bin/createuser",
                    dirname=dirname,
                    options=options,
                )

                # Create a database instance that will manage the connection and execute queries
                database = pool.PooledPostgresqlExtDatabase(
                    config["POSTGRES_DATABASE_NAME"],
                    host=PostgresHelper.postgres_db.pg_socket_dir,
                    user=PostgresHelper.postgres_db.current_user,
                    max_connections=8,
                )

                PostgresHelper._is_test = True
            else:
                # Create a database instance that will manage the connection and execute queries
                database = pool.PooledPostgresqlExtDatabase(
                    config["POSTGRES_DATABASE_NAME"],
                    host=config["POSTGRES_DATABASE_HOST"],
                    port=config["POSTGRES_DATABASE_PORT"],
                    user=config["POSTGRES_DATABASE_USERNAME"],
                    password=config["POSTGRES_DATABASE_PASSWORD"],
                    max_connections=40,
                    timeout=300,  # 5 minutes.
                    stale_timeout=300,  # 5 minutes.
                )

        # if config["TESTING"]:
        #     Bootstrapper.is_test = True

        # if bootstrap:
        #     from discograph.library.postgres.postgres_bootstrapper import (
        #         PostgresBootstrapper,
        #     )
        #
        #     PostgresBootstrapper.load_models()

        return database

    @staticmethod
    def shutdown_database():
        log.info("Shutting down database")
        if PostgresHelper.postgres_db is not None:
            log.info("Cleaning up Postgres Database")
            PostgresHelper.postgres_db.cleanup()
            if PostgresHelper._is_test:
                log.info(f"Delete data dir: {PostgresHelper.postgres_db.pg_data_dir}")
                shutil.rmtree(PostgresHelper.postgres_db.pg_data_dir)
                log.info(
                    f"Delete socket dir: {PostgresHelper.postgres_db.pg_socket_dir}"
                )
                shutil.rmtree(PostgresHelper.postgres_db.pg_socket_dir)
            PostgresHelper.postgres_db = None
        # if bootstrap_database is not None:
        #     bootstrap_database = None

    @staticmethod
    def check_connection(config, database):
        try:
            log.info("Check Postgres database connection...")

            if config["PRODUCTION"] or config["TESTING"] is False:
                connection = psycopg2.connect(
                    user=config["POSTGRES_DATABASE_USERNAME"],
                    password=config["POSTGRES_DATABASE_PASSWORD"],
                    host=config["POSTGRES_DATABASE_HOST"],
                    port=config["POSTGRES_DATABASE_PORT"],
                    database=config["POSTGRES_DATABASE_NAME"],
                )
                cursor = connection.cursor()
            else:
                cursor = database.cursor()
            cursor.execute("SELECT version();")
            record = cursor.fetchone()

            log.info(f"Database Version: {record}")

            log.info("Database connected OK.")
        except psycopg2.Error as e:
            log.exception("Error: %s" % e)

    @staticmethod
    def get_entity(entity_type: EntityType, entity_id: int):
        where_clause = PostgresEntity.entity_id == entity_id
        where_clause &= PostgresEntity.entity_type == entity_type
        with DiscogsModel.connection_context():
            query = PostgresEntity.select().where(where_clause)
            return query.get_or_none()

    # @staticmethod
    # def get_entity(entity_type: EntityType, entity_id: int):
    #     where_clause = PostgresEntity.entity_id == entity_id
    #     where_clause &= PostgresEntity.entity_type == entity_type
    #     with DiscogsModel.connection_context():
    #         query = PostgresEntity.select().where(where_clause)
    #         if not query.count():
    #             return None
    #         return query.get()

    @staticmethod
    def get_network(
        entity_id: int, entity_type: EntityType, on_mobile=False, roles=None
    ):
        from discograph.library.cache.cache_manager import cache

        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        template = "discograph:/api/{entity_type}/network/{entity_id}"
        if on_mobile:
            template += "/mobile"

        cache_key = PostgresRelationGrapher.make_cache_key(
            template,
            entity_type,
            entity_id,
            roles=roles,
        )
        cache_key = cache_key.format(entity_type, entity_id)
        log.debug(f"  get cache_key: {cache_key}")
        data = cache.get(cache_key)
        if data is not None:
            return data
        # entity_type = entity_name_types[entity_type]
        entity = PostgresHelper.get_entity(entity_type, entity_id)
        if entity is None:
            return None
        if not on_mobile:
            max_nodes = DatabaseHelper.MAX_NODES
            degree = DatabaseHelper.MAX_DEGREE
        else:
            max_nodes = DatabaseHelper.MAX_NODES_MOBILE
            degree = DatabaseHelper.MAX_DEGREE_MOBILE
        relation_grapher = PostgresRelationGrapher(
            center_entity=entity,
            degree=degree,
            max_nodes=max_nodes,
            roles=roles,
        )
        with DiscogsModel.connection_context():
            data = relation_grapher()
        cache.set(cache_key, data)
        return data

    @staticmethod
    def get_random_entity(roles=None):
        structural_roles = [
            "Alias",
            "Member Of",
            "Sublabel Of",
        ]
        with DiscogsModel.connection_context():
            if roles and any(_ not in structural_roles for _ in roles):
                relation = PostgresRelation.get_random(roles=roles)
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
                    entity: PostgresEntity = PostgresEntity.get_random()
                    relation_counts = entity.relation_counts
                    entities = entity.entities
                    # log.debug(f"relation_counts: {relation_counts}")
                    counter = counter + 1
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
                    if entity.entity_type == EntityType.LABEL:
                        log.debug("random skip label")
                        continue
                    if counter >= 1000:
                        log.debug("random count expired")
                        break

                entity_type, entity_id = entity.entity_type, entity.entity_id
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        return entity_type, entity_id

    @staticmethod
    def get_relations(entity_id: int, entity_type: EntityType):
        entity = PostgresHelper.get_entity(entity_type, entity_id)
        if entity is None:
            return None
        with DiscogsModel.connection_context():
            query = PostgresRelation.search(
                entity_id=entity.entity_id,
                entity_type=entity.entity_type,
                query_only=True,
            )
        query = query.order_by(
            PostgresRelation.role,
            PostgresRelation.entity_one_id,
            PostgresRelation.entity_one_type,
            PostgresRelation.entity_two_id,
            PostgresRelation.entity_two_type,
        )
        data = []
        for relation in query:
            category = CreditRole.all_credit_roles[relation.role]
            if category is None:
                continue
            datum = {
                "role": relation.role,
            }
            data.append(datum)
        data = {"results": tuple(data)}
        return data

    @staticmethod
    def parse_request_args(args):
        from discograph.utils import ARG_ROLES_REGEX

        year = None
        roles = set()
        for key in args:
            if key == "year":
                year = args[key]
                try:
                    if "-" in year:
                        start, _, stop = year.partition("-")
                        year = tuple(sorted((int(start), int(stop))))
                    else:
                        year = int(year)
                finally:
                    pass
            elif ARG_ROLES_REGEX.match(key):
                value = args.getlist(key)
                for role in value:
                    if role in CreditRole.all_credit_roles:
                        roles.add(role)
        roles = list(sorted(roles))
        return roles, year

    @staticmethod
    def search_entities(search_string):
        from discograph.utils import URLIFY_REGEX
        from discograph.library.cache.cache_manager import cache

        cache_key = f"discograph:/api/search/{URLIFY_REGEX.sub('+', search_string)}"
        log.debug(f"  get cache_key: {cache_key}")
        data = cache.get(cache_key)
        if data is not None:
            log.debug(f"{cache_key}: CACHED")
            for datum in data["results"]:
                log.debug(f"    {datum}")
            return data
        with DiscogsModel.connection_context():
            query = PostgresEntity.search_text(search_string)
            log.debug(f"{cache_key}: NOT CACHED")
            data = []
            for entity in query:
                datum = dict(
                    key=f"{entity.entity_type.name.lower()}-{entity.entity_id}",
                    name=entity.name,
                )
                data.append(datum)
                # log.debug(f"    {datum}")
        data = {"results": tuple(data)}
        log.debug(f"  set cache_key: {cache_key} data: {data}")
        cache.set(cache_key, data)
        return data
