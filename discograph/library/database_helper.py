from abc import ABC, abstractmethod

from discograph.library import EntityType


class DatabaseHelper(ABC):

    @staticmethod
    @abstractmethod
    def get_entity(entity_type: EntityType, entity_id: int):
        pass

    @staticmethod
    @abstractmethod
    def get_network(entity_id: int, entity_type: EntityType, on_mobile=False, cache=True, roles=None):
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
    def search_entities(search_string: str, cache=True):
        pass
