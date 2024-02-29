from abc import abstractmethod, ABC

from peewee import Database

from discograph.config import Configuration


class DatabaseWorker(ABC):
    worker_database: Database

    @staticmethod
    @abstractmethod
    def setup_worker_database(config: Configuration) -> Database:
        pass

    @staticmethod
    @abstractmethod
    def shutdown_worker_database():
        pass
