from abc import abstractmethod, ABC

from peewee import Database

from discograph.config import Configuration


class DatabaseLoader(ABC):
    loader_database: Database

    @staticmethod
    @abstractmethod
    def setup_loader_database(config: Configuration) -> Database:
        pass

    @staticmethod
    @abstractmethod
    def shutdown_loader_database():
        pass

    @staticmethod
    @abstractmethod
    def load_tables(date: str):
        pass

    @staticmethod
    @abstractmethod
    def update_tables(date: str):
        pass

    @staticmethod
    @abstractmethod
    def create_tables():
        pass

    @staticmethod
    @abstractmethod
    def drop_tables():
        pass
