import logging
from abc import ABC, abstractmethod
from typing import Type

from sqlalchemy import Engine, Index
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session, scoped_session

from discograph.config import Configuration
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


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
    def shutdown_database():
        pass

    @classmethod
    def initialize(cls):
        """ensure the parent proc's database connections are not touched
        in the new connection pool"""
        cls.engine.dispose(close=False)

    @staticmethod
    @abstractmethod
    def check_connection(config: Configuration, engine: Engine):
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
    def create_tables(cls, tables=None):
        Base.metadata.create_all(cls.engine, checkfirst=True, tables=tables)

    @classmethod
    @abstractmethod
    def drop_tables(cls):
        Base.metadata.drop_all(cls.engine, checkfirst=True)

    @classmethod
    def get_entity(cls, session: Session, entity_id: int, entity_type: EntityType):
        from discograph.library.models.entity import Entity

        pk = (entity_id, entity_type)
        return session.get(Entity, pk)

    @staticmethod
    @abstractmethod
    def get_network(
        session: Session,
        entity_id: int,
        entity_type: EntityType,
        on_mobile=False,
        roles=None,
    ):
        pass

    @staticmethod
    @abstractmethod
    def get_random_entity(session: Session, roles=None) -> tuple[int, str]:
        pass

    @staticmethod
    @abstractmethod
    def get_relations(session: Session, entity_id: int, entity_type: EntityType):
        pass

    @staticmethod
    @abstractmethod
    def search_entities(session: Session, search_string: str):
        pass
