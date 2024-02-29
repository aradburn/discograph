import logging
import random

import peewee
from peewee import Database
from playhouse.cockroachdb import PooledCockroachDatabase

from discograph.config import Configuration
from discograph.library.cockroach.cockroach_entity import CockroachEntity
from discograph.library.cockroach.cockroach_relation import CockroachRelation
from discograph.library.cockroach.cockroach_relation_grapher import (
    CockroachRelationGrapher,
)
from discograph.library.credit_role import CreditRole
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.discogs_model import DiscogsModel
from discograph.library.entity_type import EntityType

log = logging.getLogger(__name__)


class CockroachHelper(DatabaseHelper):
    @staticmethod
    def setup_database(config: Configuration) -> Database:
        log.info("Using Cockroach Database")

        database = PooledCockroachDatabase(
            config["COCKROACH_DATABASE_NAME"],
            user="root",
            host="localhost",
            # sslmode='verify-full',
            # sslrootcert='/opt/cockroachdb/certs/ca.crt',
            # sslcert='/opt/cockroachdb/certs/client.root.crt',
            # sslkey='/opt/cockroachdb/certs/client.root.key',
            max_connections=8,
        )

        return database

    @staticmethod
    def shutdown_database():
        pass

    @staticmethod
    def check_connection(config: Configuration, database: Database):
        try:
            log.info("Check Postgres database connection...")

            cursor = database.cursor()
            cursor.execute("SELECT 1")

            log.info("Database connected OK.")
        except peewee.OperationalError as e:
            log.exception(f"Error in check_connection: {e}")
            raise e

    @staticmethod
    def load_tables(date: str):
        from discograph.library.cockroach.cockroach_entity import CockroachEntity
        from discograph.library.cockroach.cockroach_relation import CockroachRelation
        from discograph.library.cockroach.cockroach_release import CockroachRelease

        log.info("Load CockroachDB tables")

        log.debug("Load entity pass 1")
        CockroachEntity.loader_pass_one(date)

        log.debug("Load release pass 1")
        CockroachRelease.loader_pass_one(date)

        log.debug("Load entity pass 2")
        CockroachEntity.loader_pass_two()

        log.debug("Load release pass 2")
        CockroachRelease.loader_pass_two()

        log.debug("Load relation pass 1")
        CockroachRelation.loader_pass_one(date)

        log.debug("Load entity pass 3")
        CockroachEntity.loader_pass_three()

        log.info("Load CockroachDB done.")

    @staticmethod
    def update_tables(date: str):
        from discograph.library.cockroach.cockroach_entity import CockroachEntity
        from discograph.library.cockroach.cockroach_relation import CockroachRelation
        from discograph.library.cockroach.cockroach_release import CockroachRelease

        log.info("Update CockroachDB tables")

        log.debug("Update entity pass 1")
        CockroachEntity.updater_pass_one(date)

        log.debug("Update release pass 1")
        CockroachRelease.updater_pass_one(date)

        log.debug("Update entity pass 2")
        CockroachEntity.loader_pass_two()

        log.debug("Update release pass 2")
        CockroachRelease.loader_pass_two()

        log.debug("Update relation pass 1")
        CockroachRelation.loader_pass_one(date)

        log.debug("Update entity pass 3")
        CockroachEntity.loader_pass_three()

        log.info("Update CockroachDB done.")

    @staticmethod
    def create_tables():
        from discograph.library.cockroach.cockroach_entity import CockroachEntity
        from discograph.library.cockroach.cockroach_relation import CockroachRelation
        from discograph.library.cockroach.cockroach_release import CockroachRelease

        log.info("Create CockroachDB tables")

        # Set parameter to True so that the create table query
        # will include an IF NOT EXISTS clause.
        log.debug("Create entity add index 1")
        entity_idx1 = CockroachEntity.index(
            CockroachEntity.entity_type, CockroachEntity.name
        )
        CockroachEntity.add_index(entity_idx1)
        log.debug("Create entity add index 2")
        entity_idx2 = CockroachEntity.index(CockroachEntity.name)
        CockroachEntity.add_index(entity_idx2)
        log.debug("Create entity add index 3")
        entity_idx3 = CockroachEntity.index(CockroachEntity.search_content)
        CockroachEntity.add_index(entity_idx3)

        CockroachEntity.create_table(True)
        CockroachRelease.create_table(True)
        CockroachRelation.create_table(True)

    @staticmethod
    def drop_tables():
        from discograph.library.cockroach.cockroach_entity import CockroachEntity
        from discograph.library.cockroach.cockroach_relation import CockroachRelation
        from discograph.library.cockroach.cockroach_release import CockroachRelease

        log.info("Drop CockroachDB tables")

        try:
            CockroachEntity.drop_table(True)
            CockroachRelease.drop_table(True)
            CockroachRelation.drop_table(True)
        except peewee.OperationalError:
            log.error("Cannot connect to Cockroach Database")

    @staticmethod
    def get_entity(entity_type: EntityType, entity_id: int):
        where_clause = CockroachEntity.entity_id == entity_id
        where_clause &= CockroachEntity.entity_type == entity_type
        with DiscogsModel.connection_context():
            query = CockroachEntity.select().where(where_clause)
            return query.get_or_none()

    @staticmethod
    def get_network(
        entity_id: int, entity_type: EntityType, on_mobile=False, roles=None
    ):
        from discograph.library.cache.cache_manager import cache

        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        template = "discograph:/api/{entity_type}/network/{entity_id}"
        if on_mobile:
            template += "/mobile"

        cache_key = CockroachRelationGrapher.make_cache_key(
            template,
            entity_type,
            entity_id,
            roles=roles,
        )
        cache_key = cache_key.format(entity_type, entity_id)
        data = cache.get(cache_key)
        if data is not None:
            return data
        # entity_type = entity_name_types[entity_type]
        entity = CockroachHelper.get_entity(entity_type, entity_id)
        if entity is None:
            return None
        if not on_mobile:
            max_nodes = DatabaseHelper.MAX_NODES
            degree = DatabaseHelper.MAX_DEGREE
        else:
            max_nodes = DatabaseHelper.MAX_NODES_MOBILE
            degree = DatabaseHelper.MAX_DEGREE_MOBILE
        relation_grapher = CockroachRelationGrapher(
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
                relation = CockroachRelation.get_random(roles=roles)
                entity_choice = random.randint(1, 2)
                if entity_choice == 1:
                    entity_type = relation.entity_one_type
                    entity_id = relation.entity_one_id
                else:
                    entity_type = relation.entity_two_type
                    entity_id = relation.entity_two_id
            else:
                entity = CockroachEntity.get_random()
                entity_type, entity_id = entity.entity_type, entity.entity_id
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        return entity_type, entity_id

    @staticmethod
    def get_relations(entity_id: int, entity_type: EntityType):
        entity = CockroachHelper.get_entity(entity_type, entity_id)
        if entity is None:
            return None
        with DiscogsModel.connection_context():
            query = CockroachRelation.search(
                entity_id=entity.entity_id,
                entity_type=entity.entity_type,
                query_only=True,
            )
        query = query.order_by(
            CockroachRelation.role,
            CockroachRelation.entity_one_id,
            CockroachRelation.entity_one_type,
            CockroachRelation.entity_two_id,
            CockroachRelation.entity_two_type,
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
            query = CockroachEntity.search_text(search_string)
            log.debug(f"{cache_key}: NOT CACHED")
            data = []
            for entity in query:
                datum = dict(
                    key=f"{entity.entity_type.name.lower()}-{entity.entity_id}",
                    name=entity.name,
                )
                data.append(datum)
                log.debug(f"    {datum}")
        data = {"results": tuple(data)}
        log.debug(f"  set cache_key: {cache_key} data: {data}")
        cache.set(cache_key, data)
        return data
