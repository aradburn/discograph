import logging

from discograph.exceptions import NotFoundError
from discograph.library.cache.cache_manager import CacheManager
from discograph.library.fields.entity_id import to_entity_label_internal_id
from discograph.library.fields.entity_type import EntityType
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.logging_config import LOGGING_TRACE
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.domain.entity import Entity
from discograph.offline.domain.release import Release
from discograph.offline.loader.loader_base import LoaderBase

# TODO tidy up
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
            if "id" in entry:
                id_ = entry["id"]
            else:
                entity_type = EntityType.LABEL
                entity_name = entry["name"]
                id_ = entity_repository.get_entity_id_by_entity_type_and_entity_name(
                    entity_type, entity_name
                )
            entry["id"] = to_entity_label_internal_id(id_)
            changed = True

        for entry in release.companies:
            if "id" in entry:
                id_ = entry["id"]
            else:
                entity_type = EntityType.LABEL
                entity_name = entry["name"]
                id_ = entity_repository.get_entity_id_by_entity_type_and_entity_name(
                    entity_type, entity_name
                )
            entry["id"] = to_entity_label_internal_id(id_)
            changed = True

        return changed

    @staticmethod
    def get_id_by_entity_type_and_entity_name(
        entity_repository: EntityRepository,
        entity_type: EntityType,
        entity_name: str,
    ) -> int | None:
        cache = CacheManager.get_cache()

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
    def init_text_search_index(
        entity_repository: EntityRepository, index: TextSearchIndex
    ) -> None:
        count = 0
        for id_, entity_name in entity_repository.all_ids_and_names():
            index.index_entry(id_, entity_name)
            count += 1
            if count % (LoaderBase.BULK_REPORTING_SIZE * 100) == 0:
                log.debug(f"Indexed {count} entities")
        # index.print_sizes()
