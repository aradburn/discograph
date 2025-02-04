import logging
import re

from discograph.utils import calculate_size

log = logging.getLogger(__name__)


class EntityDetailsIndex:

    def __init__(self):
        self.entity_countries: dict[int, list[int]] = {}
        self.countries_list: list[str] = []
        # self.genres_index: dict[int, set[str]] = {}
        # self.styles_index: dict[int, set[str]] = {}

    def index_country(self, id_: int, country: str) -> None:
        for token in re.split(r"[&,]", country):
            normalized_token = token.strip()
            if normalized_token == "":
                continue
            if id_ not in self.entity_countries:
                self.entity_countries[id_] = []
                # self.countries_index[id_] = set[str]()
            if normalized_token not in self.countries_list:
                self.countries_list.append(normalized_token)
            country_index = self.countries_list.index(normalized_token)
            self.entity_countries[id_].append(country_index)
            # log.debug(f"details add: {id_}: {self.countries_index[id_]}")

    def print_sizes(self) -> None:
        size_entity_countries = calculate_size(self.entity_countries)
        log.debug(f"size of entity_countries    : {size_entity_countries}")
        size_countries_list = calculate_size(self.countries_list)
        log.debug(f"size of countries_list    : {size_countries_list}")
