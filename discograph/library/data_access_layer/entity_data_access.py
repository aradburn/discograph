import logging
import re
from math import tanh
from typing import Dict, cast

import rapidfuzz

from discograph import utils
from discograph.exceptions import NotFoundError
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.domain.entity import Entity
from discograph.library.domain.relation import RelationResult
from discograph.library.domain.release import Release
from discograph.library.fields.entity_type import EntityType
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.library.loader.loader_base import LoaderBase
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class EntityDataAccess:
    CACHE_ENTRY_IS_NULL = "___"
    CACHE_KEY_SEPARATOR = "_"

    @staticmethod
    def resolve_entity_references(
        entity_repository: EntityRepository, entity: Entity
    ) -> bool:
        if not entity.entities:
            return False

        changed = False
        if entity.entity_type == EntityType.ARTIST:
            for section in ("aliases", "groups", "members"):
                if section not in entity.entities:
                    continue
                for entity_name in entity.entities[section].keys():
                    id_ = EntityDataAccess.get_id_by_entity_type_and_entity_name(
                        entity_repository, entity.entity_type, entity_name
                    )
                    if id_:
                        entity.entities[section][entity_name] = id_
                        changed = True
        elif entity.entity_type == EntityType.LABEL:
            for section in ("parent_label", "sublabels"):
                if section not in entity.entities:
                    continue
                for entity_name in entity.entities[section].keys():
                    id_ = EntityDataAccess.get_id_by_entity_type_and_entity_name(
                        entity_repository, entity.entity_type, entity_name
                    )
                    if id_:
                        entity.entities[section][entity_name] = id_
                        changed = True
        else:
            raise ValueError("Bad entity_type")
        return changed

    @staticmethod
    def resolve_release_references(
        entity_repository: EntityRepository, release: Release
    ):
        changed = False

        # for entry in release.artists:
        #     entity_type = EntityType.ARTIST
        #     entity_id = entry["id"]
        #     id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_id(
        #         entity_repository, entity_type, entity_id
        #     )
        #     if id_:
        #         entry["id"] = id_
        #     else:
        #         entry["id"] = -entity_id
        #     changed = True

        # for entry in release.companies:
        #     entity_type = EntityType.LABEL
        #     entity_id = entry["id"]
        #     id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_id(
        #         entity_repository, entity_type, entity_id
        #     )
        #     if id_:
        #         entry["id"] = id_
        #     else:
        #         entry["id"] = -entity_id - 1000000000
        #     changed = True
        #
        # for entry in release.extra_artists:
        #     entity_type = EntityType.ARTIST
        #     entity_id = entry["id"]
        #     id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_id(
        #         entity_repository, entity_type, entity_id
        #     )
        #     if id_:
        #         entry["id"] = id_
        #     else:
        #         entry["id"] = -entity_id
        #     changed = True
        #
        # for entry in release.tracklist:
        #     if "artists" in entry:
        #         artists_list = entry["artists"]
        #         for artist_entry in artists_list:
        #             entity_type = EntityType.ARTIST
        #             entity_id = artist_entry["id"]
        #             id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_id(
        #                 entity_repository, entity_type, entity_id
        #             )
        #             if id_:
        #                 artist_entry["id"] = id_
        #             else:
        #                 artist_entry["id"] = -entity_id
        #             changed = True
        #     if "extra_artists" in entry:
        #         extra_artists_list = entry["extra_artists"]
        #         for extra_artist_entry in extra_artists_list:
        #             entity_type = EntityType.ARTIST
        #             entity_id = extra_artist_entry["id"]
        #             id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_id(
        #                 entity_repository, entity_type, entity_id
        #             )
        #             if id_:
        #                 extra_artist_entry["id"] = id_
        #             else:
        #                 extra_artist_entry["id"] = -entity_id
        #             changed = True

        for entry in release.labels:
            entity_type = EntityType.LABEL
            entity_name = entry["name"]
            id_ = entity_repository.get_entity_id_by_entity_type_and_entity_name(
                entity_type, entity_name
            )
            entry["id"] = Entity.to_entity_label_id(id_)
            changed = True

        # for entry in release.companies:
        #     entity_type = EntityType.LABEL
        #     entity_name = entry["name"]
        #     id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_name(
        #         entity_repository, entity_type, entity_name
        #     )
        #     if id_:
        #         entry["id"] = id_
        #     else:
        #         entity_type = EntityType.ARTIST
        #         entity_name = entry["name"]
        #         id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_name(
        #             entity_repository, entity_type, entity_name
        #         )
        #         if id_:
        #             entry["id"] = id_
        #         else:
        #             entry["id"] = -1000000001
        #     changed = True

        return changed

    # @staticmethod
    # def resolve_references_from_release(
    #     entity_repository: EntityRepository, release: Release, corpus, spuriously=False
    # ):
    #     changed = False
    #     spurious_id = 0
    #     for entry in release.labels:
    #         name = entry["name"]
    #         entity_key = (EntityType.LABEL, name)
    #         if not spuriously:
    #             # release_class_name = release.__class__.__qualname__
    #             # release_module_name = release.__class__.__module__
    #             # entity_class_name = release_class_name.replace("Release", "Entity")
    #             # entity_module_name = release_module_name.replace("release", "entity")
    #             # entity_class = getattr(
    #             #     sys.modules[entity_module_name], entity_class_name
    #             # )
    #
    #             EntityDataAccess.update_corpus(entity_repository, corpus, entity_key)
    #         if entity_key in corpus:
    #             entry["id"] = corpus[entity_key]
    #             changed = True
    #         elif spuriously:
    #             spurious_id -= 1
    #             corpus[entity_key] = spurious_id
    #             entry["id"] = corpus[entity_key]
    #             changed = True
    #     return changed

    @staticmethod
    def get_id_by_entity_type_and_entity_name(
        entity_repository: EntityRepository,
        entity_type: EntityType,
        entity_name: str,
    ) -> int | None:
        from discograph.library.cache.cache_manager import cache

        entity_key_str = (
            f"{entity_name}{EntityDataAccess.CACHE_KEY_SEPARATOR}{entity_type}"
        )

        id_ = cache.get(entity_key_str)
        if id_ == EntityDataAccess.CACHE_ENTRY_IS_NULL:
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
                cache.set(entity_key_str, EntityDataAccess.CACHE_ENTRY_IS_NULL)

        return id_

    # @staticmethod
    # def update_corpus(
    #     entity_repository: EntityRepository,
    #     corpus: Dict,
    #     entity_key: Tuple[EntityType, str],
    # ):
    #     from discograph.library.cache.cache_manager import cache
    #
    #     # log.debug(f"            corpus before: {corpus}")
    #     if entity_key in corpus:
    #         return
    #
    #     entity_type, entity_name = entity_key
    #     entity_key_str = f"{entity_name}-{entity_type}"
    #     entity_id = cache.get(entity_key_str)
    #     if entity_id == "###":
    #         return
    #
    #     # if entity_id is not None:
    #     #     log.debug(f"cache hit for {key_str}")
    #     if entity_id is None:
    #         # log.debug(f"not cached, try db")
    #         try:
    #             entity = entity_repository.get_by_type_and_name(
    #                 entity_type, entity_name
    #             )
    #             # Store the internal id, not entity_id
    #             cache.set(entity_key_str, entity.id)
    #             # log.debug(f"cache set for {key_str} -> {entity_id}")
    #
    #         except NotFoundError:
    #             if LOGGING_TRACE:
    #                 log.debug(f"update_corpus key not found: {entity_key}")
    #             entity_id = None
    #             cache.set(entity_key_str, "###")
    #
    #     if entity_id is not None:
    #         corpus[entity_key] = entity_id
    #     # else:
    #     #     log.debug(f"entity_id is None")
    #     # log.debug(f"            corpus after : {corpus}")

    @staticmethod
    def roles_to_relation_count(entity: Entity, roles) -> int:
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
        return count

    @staticmethod
    def structural_roles_to_relations(
        entity: Entity, roles
    ) -> Dict[str, RelationResult]:
        # log.debug(f"            structural_roles_to_relations entity: {self}")
        # log.debug(
        #     f"            structural_roles_to_relations entities: {self.entities}"
        # )
        # log.debug(f"            structural_roles_to_relations roles: {roles}")
        relations: Dict[str, RelationResult] = {}
        if entity.entity_type == EntityType.ARTIST:
            role = "Alias"
            if role in roles and "aliases" in entity.entities:
                for entity_id in entity.entities["aliases"].values():
                    if not entity_id:
                        continue
                    ids = sorted((entity_id, entity.entity_id))
                    relation = RelationResult(
                        id=0,
                        entity_one_id=ids[0],
                        entity_one_type=entity.entity_type,
                        entity_two_id=ids[1],
                        entity_two_type=entity.entity_type,
                        random=0.0,
                        releases=None,
                        role=role,
                        distance=None,
                        pages=None,
                    )
                    relations[relation.link_key] = relation
            role = "Member Of"
            if role in roles:
                if "groups" in entity.entities:
                    for entity_id in entity.entities["groups"].values():
                        if not entity_id:
                            continue
                        relation = RelationResult(
                            id=0,
                            entity_one_id=entity.entity_id,
                            entity_one_type=entity.entity_type,
                            entity_two_id=entity_id,
                            entity_two_type=entity.entity_type,
                            random=0.0,
                            releases=None,
                            role=role,
                            distance=None,
                            pages=None,
                        )
                        relations[relation.link_key] = relation
                if "members" in entity.entities:
                    for entity_id in entity.entities["members"].values():
                        if not entity_id:
                            continue
                        relation = RelationResult(
                            id=0,
                            entity_one_id=entity_id,
                            entity_one_type=entity.entity_type,
                            entity_two_id=entity.entity_id,
                            entity_two_type=entity.entity_type,
                            random=0.0,
                            releases=None,
                            role=role,
                            distance=None,
                            pages=None,
                        )
                        relations[relation.link_key] = relation
        elif entity.entity_type == EntityType.LABEL and "Sublabel Of" in roles:
            role = "Sublabel Of"
            if "parent_label" in entity.entities:
                for entity_id in entity.entities["parent_label"].values():
                    if not entity_id:
                        continue
                    relation = RelationResult(
                        id=0,
                        entity_one_id=entity.entity_id,
                        entity_one_type=entity.entity_type,
                        entity_two_id=entity_id,
                        entity_two_type=entity.entity_type,
                        random=0.0,
                        releases=None,
                        role=role,
                        distance=None,
                        pages=None,
                    )
                    relations[relation.link_key] = relation
            if "sublabels" in entity.entities:
                for entity_id in entity.entities["sublabels"].values():
                    if not entity_id:
                        continue
                    relation = RelationResult(
                        id=0,
                        entity_one_id=entity_id,
                        entity_one_type=entity.entity_type,
                        entity_two_id=entity.entity_id,
                        entity_two_type=entity.entity_type,
                        random=0.0,
                        releases=None,
                        role=role,
                        distance=None,
                        pages=None,
                    )
                    relations[relation.link_key] = relation
        # log.debug(f"            structural_roles_to_relations relations: {relations}")
        return relations

    @staticmethod
    def normalise_search_content(string: str) -> str:
        string = string.lower()
        string = utils.to_ascii(string)
        string = utils.STRIP_PATTERN.sub("", string)
        string = string.replace("not on label", "")
        string = string.replace("self released", "")
        string = string.replace("self-released", "")
        string = string.replace("(", "")
        string = string.replace(")", "")
        string = string.replace("&", "")
        string = string.replace('"', "")
        string = string.replace(".", "")
        string = string.replace(",", "")
        string = string.strip()
        return string

    @staticmethod
    def init_text_search_index(
        entity_repository: EntityRepository, index: TextSearchIndex
    ) -> None:
        count = 0
        for id_, entity_name in entity_repository.all_ids_and_names():
            index.index_entry(id_, entity_name)
            count += 1
            if count % (LoaderBase.BULK_REPORTING_SIZE * 100) == 0:
                log.debug(f"Indexed {count} entities")
        index.print_sizes()

    @staticmethod
    def search_entities(search_string):
        from discograph.utils import URLIFY_REGEX
        from discograph.library.cache.cache_manager import cache

        normalised_search_string = EntityDataAccess.normalise_search_content(
            search_string
        )

        search_query_url = URLIFY_REGEX.sub("+", normalised_search_string)
        cache_key = f"discograph:/api/search/{search_query_url}"
        # log.debug(f"  get cache_key: {cache_key}")
        data = cache.get(cache_key)
        if data is not None:
            log.debug(f"{cache_key}: CACHED")
            # for datum in data["results"]:
            #     log.debug(f"    {datum}")
            return data

        documents = DatabaseHelper.db_helper.search_text_index(normalised_search_string)
        # log.debug(f"search results: {documents}")

        sorted_documents = EntityDataAccess.sort_search_results(
            search_string, documents
        )

        # log.debug(f"{cache_key}: NOT CACHED")
        data = []
        for document in sorted_documents:
            entity_id, entity_type = Entity.to_entity_external_id(document[0])
            json_entity_key = Entity.to_json_entity_key(entity_id, entity_type)
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
                    score += 1.0 + (100.0 - int(digits))

            # Boost candidates that start with the given search string
            if candidate_name.lower().startswith(search_string.lower()):
                score += 1

            # Boost candidates that are an exact match
            if candidate_name.lower() == search_string.lower():
                score += 100.0

            # Penalise candidates that differ in length (longer or shorter)
            len_diff = abs(len(candidate_name) - len(search_string)) / 100.0
            score -= len_diff

            scored_documents.append((score, document))
        sorted_documents = sorted(
            scored_documents,
            key=lambda scored_document: scored_document[0],
            reverse=True,
        )
        result_documents = [sorted_document[1] for sorted_document in sorted_documents]
        return result_documents
