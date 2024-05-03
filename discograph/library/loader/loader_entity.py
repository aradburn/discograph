import logging
from random import random

from discograph.database import get_concurrency_count
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.entity_table import EntityTable
from discograph.library.database.transaction import transaction
from discograph.library.domain.entity import Entity
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader.worker_entity_pass_three import WorkerEntityPassThree
from discograph.library.loader.worker_entity_pass_two import WorkerEntityPassTwo
from discograph.library.loader_utils import LoaderUtils
from discograph.logging_config import LOGGING_TRACE
from discograph.utils import normalise_search_content, timeit

log = logging.getLogger(__name__)


class LoaderEntity(LoaderBase):
    # CLASS METHODS

    @classmethod
    @timeit
    def loader_pass_one(cls, date: str) -> int:
        log.debug("entity loader pass one - artist")
        with transaction():
            entity_repository = EntityRepository()
            artists_loaded = cls.loader_pass_one_manager(
                repository=entity_repository,
                date=date,
                xml_tag="artist",
                id_attr=EntityTable.entity_id.name,
                name_attr="name",
                skip_without=["entity_name"],
            )
        log.debug("entity loader pass one - label")
        with transaction():
            entity_repository = EntityRepository()
            labels_loaded = cls.loader_pass_one_manager(
                repository=entity_repository,
                date=date,
                xml_tag="label",
                id_attr=EntityTable.entity_id.name,
                name_attr="name",
                skip_without=["entity_name"],
            )
        return artists_loaded + labels_loaded

    @classmethod
    @timeit
    def loader_pass_two(cls) -> None:
        log.debug("entity loader pass two - artist")

        with transaction():
            repository = EntityRepository()
            entity_type: EntityType = EntityType.ARTIST
            chunked_entity_ids = repository.get_chunked_entity_ids(entity_type)
            workers = [
                WorkerEntityPassTwo(entity_type, entity_ids)
                for entity_ids in chunked_entity_ids
            ]

            log.debug(f"entity loader pass two - artist start {len(workers)} workers")
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
                if worker.exitcode > 0:
                    log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                    raise Exception("Error in worker process")
            for worker in workers:
                worker.terminate()

            log.debug("entity loader pass two - label")
            entity_type: EntityType = EntityType.LABEL
            chunked_entity_ids = repository.get_chunked_entity_ids(entity_type)

            workers = [
                WorkerEntityPassTwo(entity_type, entity_ids)
                for entity_ids in chunked_entity_ids
            ]
            log.debug(f"entity loader pass two - label start {len(workers)} workers")
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
                if worker.exitcode > 0:
                    log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                    raise Exception("Error in worker process")
            for worker in workers:
                worker.terminate()
            log.debug("entity loader pass two - done")

    @classmethod
    @timeit
    def loader_pass_three(cls):
        log.debug("entity loader pass three")
        with transaction():
            entity_repository = EntityRepository()
            entity_type: EntityType = EntityType.ARTIST
            batched_release_ids = entity_repository.get_batched_ids(
                entity_type, LoaderBase.BULK_INSERT_BATCH_SIZE
            )

        workers = []
        for release_ids in batched_release_ids:
            worker = WorkerEntityPassThree(entity_type, release_ids)
            worker.start()
            workers.append(worker)

            if len(workers) > get_concurrency_count():
                worker = workers.pop(0)
                if LOGGING_TRACE:
                    log.debug(f"wait for worker {len(workers)} in list")
                worker.join()
                if worker.exitcode > 0:
                    log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                    raise Exception("Error in worker process")
                worker.terminate()

        while len(workers) > 0:
            worker = workers.pop(0)
            if LOGGING_TRACE:
                log.debug(
                    f"wait for worker {worker.name} - {len(workers)} left in list"
                )
            worker.join()
            if worker.exitcode > 0:
                log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                raise Exception("Error in worker process")
            worker.terminate()

        with transaction():
            entity_repository = EntityRepository()
            entity_type: EntityType = EntityType.LABEL
            batched_release_ids = entity_repository.get_batched_ids(
                entity_type, LoaderBase.BULK_INSERT_BATCH_SIZE
            )

        workers = []
        for release_ids in batched_release_ids:
            worker = WorkerEntityPassThree(entity_type, release_ids)
            worker.start()
            workers.append(worker)

            if len(workers) > get_concurrency_count():
                worker = workers.pop(0)
                if LOGGING_TRACE:
                    log.debug(f"wait for worker {len(workers)} in list")
                worker.join()
                if worker.exitcode > 0:
                    log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                    raise Exception("Error in worker process")
                worker.terminate()

        while len(workers) > 0:
            worker = workers.pop(0)
            if LOGGING_TRACE:
                log.debug(
                    f"wait for worker {worker.name} - {len(workers)} left in list"
                )
            worker.join()
            if worker.exitcode > 0:
                log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                raise Exception("Error in worker process")
            worker.terminate()

        # with transaction():
        #     repository = EntityRepository()
        #     entity_type: EntityType = EntityType.ARTIST
        #     chunked_entity_ids = repository.get_chunked_entity_ids(entity_type)
        #     workers = [
        #         WorkerEntityPassThree(entity_type, entity_ids)
        #         for entity_ids in chunked_entity_ids
        #     ]
        #     log.debug(f"entity loader pass three - artist start {len(workers)} workers")
        #     for worker in workers:
        #         worker.start()
        #     for worker in workers:
        #         worker.join()
        #         if worker.exitcode > 0:
        #             log.debug(
        #                 f"entity loader worker {worker.name} exitcode: {worker.exitcode}"
        #             )
        #             raise Exception("Error in worker process")
        #     for worker in workers:
        #         worker.terminate()
        #     entity_type: EntityType = EntityType.LABEL
        #     chunked_entity_ids = repository.get_chunked_entity_ids(entity_type)
        #     workers = [
        #         WorkerEntityPassThree(entity_type, entity_ids)
        #         for entity_ids in chunked_entity_ids
        #     ]
        #     log.debug(f"entity loader pass three - label start {len(workers)} workers")
        #     for worker in workers:
        #         worker.start()
        #     for worker in workers:
        #         worker.join()
        #         if worker.exitcode > 0:
        #             log.debug(
        #                 f"entity loader worker {worker.name} exitcode: {worker.exitcode}"
        #             )
        #             raise Exception("Error in worker process")
        #     for worker in workers:
        #         worker.terminate()

    @classmethod
    def element_to_names(cls, names):
        result = {}
        if names is None or not len(names):
            return result
        for name in names:
            name = name.text
            if not name:
                continue
            result[name] = None
        return result

    @classmethod
    def element_to_names_and_ids(cls, names_and_ids):
        result = {}
        if names_and_ids is None or not len(names_and_ids):
            return result
        for i in range(0, len(names_and_ids), 2):
            discogs_id = int(names_and_ids[i].text)
            name = names_and_ids[i + 1].text
            result[name] = discogs_id
        return result

    @classmethod
    def element_to_parent_label(cls, parent_label):
        result = {}
        if parent_label is None or parent_label.text is None:
            return result
        name = parent_label.text.strip()
        if not name:
            return result
        result[name] = None
        return result

    @classmethod
    def element_to_sublabels(cls, sublabels):
        result = {}
        if sublabels is None or not len(sublabels):
            return result
        for sublabel in sublabels:
            name = sublabel.text
            if name is None:
                continue
            name = name.strip()
            if not name:
                continue
            result[name] = None
        return result

    @classmethod
    def from_element(cls, element) -> Entity:
        data = cls.tags_to_fields(element)
        data["relation_counts"] = {}
        data["random"] = random()
        # log.debug(f"from_element: {cls.entity_name} element: {element} data: {data}")
        return Entity(**data)

    @classmethod
    def preprocess_data(cls, data, element):
        data["entity_metadata"] = {}
        data["entities"] = {}
        data["relation_counts"] = {}
        for key in (
            "aliases",
            "groups",
            "members",
            "parent_label",
            "sublabels",
        ):
            if key in data:
                data["entities"][key] = data.pop(key)
        for key in (
            "contact_info",
            "name_variations",
            "profile",
            "real_name",
            "urls",
        ):
            if key in data:
                data["entity_metadata"][key] = data.pop(key)
        if "entity_name" in data and data.get("entity_name"):
            name = data.get("entity_name")
            data["search_content"] = normalise_search_content(name)
        if element.tag == "artist":
            data["entity_type"] = EntityType.ARTIST
        elif element.tag == "label":
            data["entity_type"] = EntityType.LABEL
        return data

    # def roles_to_relation_count(self, roles) -> int:
    #     count = 0
    #     relation_counts = self.relation_counts or {}
    #     for role in roles:
    #         if role == "Alias":
    #             if "aliases" in self.entities:
    #                 count += len(cast(Dict, self.entities["aliases"]))
    #         elif role == "Member Of":
    #             if "groups" in self.entities:
    #                 count += len(cast(Dict, self.entities["groups"]))
    #             if "members" in self.entities:
    #                 count += len(cast(Dict, self.entities["members"]))
    #         elif role == "Sublabel Of":
    #             if "parent_label" in self.entities:
    #                 count += len(cast(Dict, self.entities["parent_label"]))
    #             if "sublabels" in self.entities:
    #                 count += len(cast(Dict, self.entities["sublabels"]))
    #         else:
    #             count += relation_counts.get(role, 0)
    #     return count


LoaderEntity._tags_to_fields_mapping = {
    "aliases": ("aliases", LoaderEntity.element_to_names),
    "contact_info": ("contact_info", LoaderUtils.element_to_string),
    "groups": ("groups", LoaderEntity.element_to_names),
    "id": ("entity_id", LoaderUtils.element_to_integer),
    "members": ("members", LoaderEntity.element_to_names_and_ids),
    "name": ("entity_name", LoaderUtils.element_to_string),
    "namevariations": ("name_variations", LoaderUtils.element_to_strings),
    "parentLabel": ("parent_label", LoaderEntity.element_to_parent_label),
    "profile": ("profile", LoaderUtils.element_to_string),
    "realname": ("real_name", LoaderUtils.element_to_string),
    "sublabels": ("sublabels", LoaderEntity.element_to_sublabels),
    "urls": ("urls", LoaderUtils.element_to_strings),
}
