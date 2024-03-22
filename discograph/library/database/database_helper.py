from abc import ABC, abstractmethod

from peewee import Database

from discograph.config import Configuration
from discograph.library.fields.entity_type import EntityType


class DatabaseHelper(ABC):
    database: Database

    MAX_NODES = 400
    MAX_NODES_MOBILE = 25

    MAX_DEGREE = 3
    # was 12
    MAX_DEGREE_MOBILE = 3

    LINK_RATIO = 10
    # was 3

    @staticmethod
    @abstractmethod
    def setup_database(config: Configuration) -> Database:
        pass

    @staticmethod
    @abstractmethod
    def shutdown_database():
        pass

    @staticmethod
    @abstractmethod
    def check_connection(config: Configuration, database: Database):
        pass

    @staticmethod
    @abstractmethod
    def load_tables(date: str):
        pass

    @staticmethod
    @abstractmethod
    def update_tables(date: str):
        pass

    @classmethod
    @abstractmethod
    def create_tables(cls):
        from discograph.library.models.genre import Genre
        from discograph.library.models.role import Role

        # Set parameter to True so that the create table query
        # will include an IF NOT EXISTS clause.
        Role.create_table(True)
        Genre.create_table(True)

    @classmethod
    def create_join_tables(cls):
        from discograph.library.models.release_genre import ReleaseGenre

        # Set parameter to True so that the create table query
        # will include an IF NOT EXISTS clause.
        ReleaseGenre.create_table(True)

    @classmethod
    @abstractmethod
    def drop_tables(cls):
        from discograph.library.models.genre import Genre
        from discograph.library.models.role import Role

        Role.drop_table(True)
        Genre.drop_table(True)

    @classmethod
    def drop_join_tables(cls):
        from discograph.library.models.release_genre import ReleaseGenre

        ReleaseGenre.drop_table(True)

    @staticmethod
    @abstractmethod
    def get_entity(entity_type: EntityType, entity_id: int):
        pass

    @staticmethod
    @abstractmethod
    def get_network(
        entity_id: int, entity_type: EntityType, on_mobile=False, roles=None
    ):
        pass

    @staticmethod
    @abstractmethod
    def get_random_entity(roles=None):
        pass

    @staticmethod
    @abstractmethod
    def get_relations(entity_id: int, entity_type: EntityType):
        pass

    @staticmethod
    @abstractmethod
    def search_entities(search_string: str):
        pass
