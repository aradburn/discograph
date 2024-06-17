import logging

from sqlalchemy import Engine, text, URL, create_engine
from sqlalchemy.exc import SQLAlchemyError, DatabaseError

from discograph.config import Configuration
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_relation import LoaderRelation
from discograph.library.loader.loader_release import LoaderRelease
from discograph.library.loader.loader_role import LoaderRole

log = logging.getLogger(__name__)


class CockroachHelper(DatabaseHelper):
    @staticmethod
    def setup_database(config: Configuration) -> Engine:
        log.info("Using Cockroach Database")

        url_object = URL.create(
            "postgresql+psycopg2",
            username="root",
            host="localhost",
            database=config["COCKROACH_DATABASE_NAME"],
        )
        ssl_args = {
            "sslmode": "verify-full",
            "sslrootcert": "/opt/cockroachdb/certs/ca.crt",
            "sslcert": "/opt/cockroachdb/certs/client.root.crt",
            "sslkey": "/opt/cockroachdb/certs/client.root.key",
        }
        engine = create_engine(
            url_object,
            pool_size=40,
            pool_timeout=300,
            pool_recycle=300,
            connect_args=ssl_args,
        )

        return engine

    @staticmethod
    def shutdown_database():
        log.info("Shutting down database")
        DatabaseHelper.engine.dispose()

    @staticmethod
    def check_connection(config: Configuration, engine: Engine):
        try:
            log.info("Check Cockroach DB database connection...")

            with engine.connect() as connection:
                connection.execute(text("SELECT 1;"))

            log.info("Database connected OK.")
        except DatabaseError as e:
            log.exception("Connection Error")
            raise e

    @staticmethod
    def load_tables(data_directory: str, date: str, is_bulk_inserts=False):
        log.info("Load CockroachDB tables")

        log.debug("Load role pass 1")
        LoaderRole().loader_pass_one(date)

        log.debug("Load entity pass 1")
        LoaderEntity().loader_pass_one(data_directory, date, is_bulk_inserts)

        log.debug("Load release pass 1")
        LoaderRelease().loader_pass_one(data_directory, date, is_bulk_inserts)

        log.debug("Load entity pass 2")
        LoaderEntity().loader_pass_two()

        log.debug("Load release pass 2")
        LoaderRelease().loader_pass_two()

        log.debug("Load relation pass 1")
        LoaderRelation().loader_relation_pass_one(date)

        # log.debug("Load relation pass 2")
        # LoaderRelation().loader_relation_pass_two(date)

        log.debug("Load entity pass 3")
        LoaderEntity().loader_pass_three()

        log.info("Load CockroachDB done.")

    @classmethod
    def create_tables(cls, tables=None):
        log.info("Create CockroachDB tables")

        # Set parameter to True so that the create table query
        # will include an IF NOT EXISTS clause.
        # log.debug("Create entity add index 1")
        # entity_idx1 = CockroachEntity.index(
        #     CockroachEntity.entity_type, CockroachEntity.name
        # )
        # CockroachEntity.add_index(entity_idx1)
        # log.debug("Create entity add index 2")
        # entity_idx2 = CockroachEntity.index(CockroachEntity.name)
        # CockroachEntity.add_index(entity_idx2)
        # log.debug("Create entity add index 3")
        # entity_idx3 = CockroachEntity.index(CockroachEntity.search_content)
        # CockroachEntity.add_index(entity_idx3)

        super().create_tables(tables=tables)

        # CockroachEntity.create_table(True)
        # CockroachRelease.create_table(True)
        # CockroachRelation.create_table(True)
        #
        # super().create_join_tables()

    @classmethod
    def drop_tables(cls):
        log.info("Drop CockroachDB tables")

        try:
            # super().drop_join_tables()
            #
            # CockroachEntity.drop_table(True)
            # CockroachRelease.drop_table(True)
            # CockroachRelation.drop_table(True)

            super().drop_tables()
        except SQLAlchemyError:
            log.error("Cannot connect to Cockroach Database")

    # @staticmethod
    # def get_entity(entity_type: EntityType, entity_id: int):
    #     where_clause = CockroachEntity.entity_id == entity_id
    #     where_clause &= CockroachEntity.entity_type == entity_type
    #     with LoaderBase.connection_context():
    #         query = CockroachEntity.select().where(where_clause)
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
    #     cache_key = RelationGrapher.make_cache_key(
    #         template,
    #         entity_id,
    #         entity_type,
    #         roles=roles,
    #     )
    #     cache_key = cache_key.format(entity_type, entity_id)
    #     data = cache.get(cache_key)
    #     if data is not None:
    #         return data
    #     # entity_type = entity_name_types[entity_type]
    #     entity = EntityRepository().get(entity_id, entity_type)
    #     if entity is None:
    #         return None
    #     if not on_mobile:
    #         max_nodes = DatabaseHelper.MAX_NODES
    #         degree = DatabaseHelper.MAX_DEGREE
    #     else:
    #         max_nodes = DatabaseHelper.MAX_NODES_MOBILE
    #         degree = DatabaseHelper.MAX_DEGREE_MOBILE
    #     relation_grapher = RelationGrapher(
    #         center_entity=entity,
    #         degree=degree,
    #         max_nodes=max_nodes,
    #         roles=roles,
    #     )
    #     data = relation_grapher.get_relation_graph(session)
    #     cache.set(cache_key, data)
    #     return data

    # @staticmethod
    # def get_random_entity(session: Session, roles=None) -> tuple[int, str]:
    #     structural_roles = [
    #         "Alias",
    #         "Member Of",
    #         "Sublabel Of",
    #     ]
    #     if roles and any(_ not in structural_roles for _ in roles):
    #         relation = RelationRepository().get_random_relation(session, roles=roles)
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
    #             entity: CockroachEntityDB = CockroachEntityDB.get_random_entity(session)
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

    # @staticmethod
    # def get_relations(session: Session, entity_id: int, entity_type: EntityType):
    #     entity = CockroachHelper.get_entity(session, entity_id, entity_type)
    #     if entity is None:
    #         return None
    #     where_clause = CockroachRelationDB.search(
    #         entity_id=entity.entity_id,
    #         entity_type=entity.entity_type,
    #         query_only=True,
    #     )
    #     relations = session.scalars(
    #         select(CockroachRelationDB)
    #         .where(where_clause)
    #         .order_by(
    #             CockroachRelationDB.role,
    #             CockroachRelationDB.entity_one_id,
    #             CockroachRelationDB.entity_one_type,
    #             CockroachRelationDB.entity_two_id,
    #             CockroachRelationDB.entity_two_type,
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
    #     query = CockroachEntityDB.build_search_text_query(search_string)
    #     log.debug(f"query: {query}")
    #     entities = session.execute(text(query)).scalars()
    #     log.debug(f"{cache_key}: NOT CACHED")
    #     data = []
    #     for entity in entities:
    #         datum = dict(
    #             key=f"{entity.entity_id}-{entity.entity_type.lower()}",
    #             name=entity.name,
    #         )
    #         data.append(datum)
    #         log.debug(f"    {datum}")
    #     data = {"results": tuple(data)}
    #     log.debug(f"  set cache_key: {cache_key} data: {data}")
    #     cache.set(cache_key, data)
    #     return data
