from abc import ABC, abstractmethod

from discograph.library import EntityType


class DatabaseHelper(ABC):

    MAX_NODES = 400
    MAX_NODES_MOBILE = 25

    MAX_DEGREE = 3
    # was 12
    MAX_DEGREE_MOBILE = 3

    LINK_RATIO = 10
    # was 3

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
