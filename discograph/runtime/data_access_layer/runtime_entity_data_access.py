import logging
import pickle
import re
from pathlib import Path
from typing import Dict, cast

import rapidfuzz

from discograph.exceptions import NotFoundError
from discograph.library.cache.cache_manager import CacheManager
from discograph.library.fields.entity_id import (
    LABEL_ENTITY_ID_OFFSET,
    to_entity_external_id,
)
from discograph.library.fields.entity_type import EntityType
from discograph.library.full_text_search.entity_details_index import EntityDetailsIndex
from discograph.library.full_text_search.text_search_utils import (
    normalise_search_content,
)
from discograph.logging_config import LOGGING_TRACE
from discograph.runtime.runtime_database.runtime_entity_repository import (
    RuntimeEntityRepository,
)
from discograph.runtime.runtime_domain.entity import RuntimeEntity
from discograph.runtime.runtime_domain.relation import RuntimeRelationResult

log = logging.getLogger(__name__)


class RuntimeEntityDataAccess:
    CACHE_ENTRY_IS_NULL = "___"
    CACHE_KEY_SEPARATOR = "_"

    @staticmethod
    def roles_to_relation_count(entity: RuntimeEntity, roles) -> int:
        count = 0
        relation_counts = entity.relation_counts or {}
        for role in roles:
            if role == "Alias":
                if "aliases" in entity.entities:
                    count += len(cast(Dict, entity.entities["aliases"]))
            elif role == "Member Of":
                if "groups" in entity.entities:
                    count += len(cast(Dict, entity.entities["groups"]))
                if "members" in entity.entities:
                    count += len(cast(Dict, entity.entities["members"]))
            elif role == "Sublabel Of":
                if "parent_label" in entity.entities:
                    count += len(cast(Dict, entity.entities["parent_label"]))
                if "sublabels" in entity.entities:
                    count += len(cast(Dict, entity.entities["sublabels"]))
            else:
                count += relation_counts.get(role, 0)
        log.debug(
            f"roles_to_relation_count entity: {entity} roles: {roles} -> {count})"
        )
        return count

    @staticmethod
    def structural_roles_to_relations(
        entity: RuntimeEntity, roles
    ) -> Dict[str, RuntimeRelationResult]:
        # log.debug(f"            structural_roles_to_relations entity: {self}")
        # log.debug(
        #     f"            structural_roles_to_relations entities: {self.entities}"
        # )
        # log.debug(f"            structural_roles_to_relations roles: {roles}")
        relations: Dict[str, RuntimeRelationResult] = {}
        if entity.entity_type == EntityType.ARTIST:
            role = "Alias"
            if role in roles and "aliases" in entity.entities:
                for entity_id in entity.entities["aliases"].values():
                    if not entity_id:
                        continue
                    ids = sorted((entity_id, entity.entity_id))
                    relation = RuntimeRelationResult(
                        id=0,
                        entity_one_id=ids[0],
                        entity_one_type=entity.entity_type,
                        entity_two_id=ids[1],
                        entity_two_type=entity.entity_type,
                        releases=None,
                        role=role,
                        distance=None,
                    )
                    relations[relation.link_key] = relation
            role = "Member Of"
            if role in roles:
                if "groups" in entity.entities:
                    for entity_id in entity.entities["groups"].values():
                        if not entity_id:
                            continue
                        relation = RuntimeRelationResult(
                            id=0,
                            entity_one_id=entity.entity_id,
                            entity_one_type=entity.entity_type,
                            entity_two_id=entity_id,
                            entity_two_type=entity.entity_type,
                            releases=None,
                            role=role,
                            distance=None,
                        )
                        relations[relation.link_key] = relation
                if "members" in entity.entities:
                    for entity_id in entity.entities["members"].values():
                        if not entity_id:
                            continue
                        relation = RuntimeRelationResult(
                            id=0,
                            entity_one_id=entity_id,
                            entity_one_type=entity.entity_type,
                            entity_two_id=entity.entity_id,
                            entity_two_type=entity.entity_type,
                            releases=None,
                            role=role,
                            distance=None,
                        )
                        relations[relation.link_key] = relation
        elif entity.entity_type == EntityType.LABEL and "Sublabel Of" in roles:
            role = "Sublabel Of"
            if "parent_label" in entity.entities:
                for entity_id in entity.entities["parent_label"].values():
                    if not entity_id:
                        continue
                    relation = RuntimeRelationResult(
                        id=0,
                        entity_one_id=entity.entity_id,
                        entity_one_type=entity.entity_type,
                        entity_two_id=entity_id,
                        entity_two_type=entity.entity_type,
                        releases=None,
                        role=role,
                        distance=None,
                    )
                    relations[relation.link_key] = relation
            if "sublabels" in entity.entities:
                for entity_id in entity.entities["sublabels"].values():
                    if not entity_id:
                        continue
                    relation = RuntimeRelationResult(
                        id=0,
                        entity_one_id=entity_id,
                        entity_one_type=entity.entity_type,
                        entity_two_id=entity.entity_id,
                        entity_two_type=entity.entity_type,
                        releases=None,
                        role=role,
                        distance=None,
                    )
                    relations[relation.link_key] = relation
        # log.debug(f"            structural_roles_to_relations relations: {relations}")
        return relations

    @staticmethod
    def search_entities(search_string):
        from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
        from discograph.utils import URLIFY_REGEX

        cache = CacheManager.get_cache()

        normalised_search_string = normalise_search_content(search_string)

        search_query_url = URLIFY_REGEX.sub("+", normalised_search_string)
        cache_key = f"discograph:/api/search/{search_query_url}"
        # log.debug(f"  get cache_key: {cache_key}")
        data = cache.get(cache_key)
        if data is not None:
            log.debug(f"{cache_key}: CACHED")
            # for datum in data["results"]:
            #     log.debug(f"    {datum}")
            return data

        documents = RuntimeDatabaseManager.runtime_database_helper.search_text_index(
            normalised_search_string
        )
        # log.debug(f"search results: {documents}")

        sorted_documents = RuntimeEntityDataAccess.sort_search_results(
            search_string, documents
        )

        # log.debug(f"{cache_key}: NOT CACHED")
        data = []
        for document in sorted_documents:
            entity_id, entity_type = to_entity_external_id(document[0])
            json_entity_key = RuntimeEntity.to_json_entity_key(entity_id, entity_type)
            datum = dict(
                key=json_entity_key,
                name=document[1],
            )
            data.append(datum)
            log.debug(f"    {datum}")
        data = {"results": tuple(data)}
        # log.debug(f"  set cache_key: {cache_key} data: {data}")
        cache.set(cache_key, data)
        return data

    @staticmethod
    def sort_search_results(
        search_string: str,
        documents: list[tuple[int, str]],
    ) -> list[tuple[int, str]]:
        scored_documents: list[tuple[float, tuple[int, str]]] = list()
        for document in documents:
            candidate_id = document[0]
            candidate_name = document[1]
            score = rapidfuzz.distance.JaroWinkler.normalized_distance(
                search_string, candidate_name
            )

            matched_digits = re.match(r"(.*) \((\d+)\)", candidate_name)

            # Boost candidates that match and order by the number in brackets
            # eg. Test (1) is better than Test (23)
            if matched_digits:
                digits = matched_digits.group(2)
                if matched_digits.group(1) == search_string:
                    score += 1.0 + (1000.0 - int(digits)) / 1000.0
                else:
                    score += (1000.0 - int(digits)) / 1000.0

            # Boost candidates that start with the given search string
            if candidate_name.lower().startswith(search_string.lower()):
                score += 1.0

            # Boost candidates that are an exact match
            if candidate_name.lower() == search_string.lower():
                score += 100.0

            # Penalise candidates that differ in length (longer or shorter)
            len_diff = abs(len(candidate_name) - len(search_string)) / 100.0
            score -= len_diff

            # Put artists before labels
            if candidate_id >= LABEL_ENTITY_ID_OFFSET:
                score -= 10.0

            scored_documents.append((score, document))
        sorted_documents = sorted(
            scored_documents,
            key=lambda scored_document: scored_document[0],
            reverse=True,
        )
        result_documents = [sorted_document[1] for sorted_document in sorted_documents]
        return result_documents

    @staticmethod
    def get_id_by_entity_type_and_entity_name(
        entity_repository: RuntimeEntityRepository,
        entity_type: EntityType,
        entity_name: str,
    ) -> int | None:
        cache = CacheManager.get_cache()

        entity_key_str = (
            f"{entity_name}{RuntimeEntityDataAccess.CACHE_KEY_SEPARATOR}{entity_type}"
        )

        id_ = cache.get(entity_key_str)
        if id_ == RuntimeEntityDataAccess.CACHE_ENTRY_IS_NULL:
            return None

        # if entity_id is not None:
        #     log.debug(f"cache hit for {key_str}")
        if id_ is None:
            # log.debug(f"not cached, try db")
            try:
                int_id = entity_repository.get_id_by_entity_type_and_entity_name(
                    entity_type, entity_name
                )
                # Store the internal id, not entity_id
                cache.set(entity_key_str, int_id)
                # log.debug(f"cache set for {key_str} -> {int_id}")
                id_ = int_id

            except NotFoundError:
                if LOGGING_TRACE:
                    log.debug(
                        f"get_id_from_entity_type_and_entity_name key not found: {entity_key_str}"
                    )
                id_ = None
                cache.set(entity_key_str, RuntimeEntityDataAccess.CACHE_ENTRY_IS_NULL)

        return id_

    @staticmethod
    def load_entity_details_index_from_file(filename: Path) -> EntityDetailsIndex:
        log.debug(f"load entity details index from file: {filename}")

        # open a file, where you stored the pickled data
        with open(filename, "rb") as file:
            # read pickle dump information from that file
            entity_details_index: EntityDetailsIndex = pickle.load(file)
            # for country in sorted(entity_details_index.countries_list):
            #     print(f"{country}")
        return entity_details_index
