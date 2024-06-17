import logging
from random import random

from discograph.database import get_concurrency_count
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.entity_table import EntityTable
from discograph.library.database.transaction import transaction
from discograph.library.domain.entity import Entity
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader.worker_entity_inserter import WorkerEntityInserter
from discograph.library.loader.worker_entity_pass_three import WorkerEntityPassThree
from discograph.library.loader.worker_entity_pass_two import WorkerEntityPassTwo
from discograph.library.loader.worker_entity_updater import WorkerEntityUpdater
from discograph.library.loader_utils import LoaderUtils
from discograph.logging_config import LOGGING_TRACE
from discograph.utils import normalise_search_content, timeit

log = logging.getLogger(__name__)


class LoaderEntity(LoaderBase):
    # CLASS METHODS

    @classmethod
    @timeit
    def loader_pass_one(
        cls, data_directory: str, data_date: str, is_bulk_inserts=False
    ) -> int:
        log.debug(f"entity loader pass one - artist - date: {data_date}")
        with transaction():
            entity_repository = EntityRepository()
            artists_loaded = cls.loader_pass_one_manager(
                repository=entity_repository,
                data_directory=data_directory,
                date=data_date,
                xml_tag="artist",
                id_attr=EntityTable.entity_id.name,
                skip_without=["entity_name"],
                is_bulk_inserts=is_bulk_inserts,
            )
        log.debug(f"entity loader pass one - label - date: {data_date}")
        with transaction():
            entity_repository = EntityRepository()
            labels_loaded = cls.loader_pass_one_manager(
                repository=entity_repository,
                data_directory=data_directory,
                date=data_date,
                xml_tag="label",
                id_attr=EntityTable.entity_id.name,
                skip_without=["entity_name"],
                is_bulk_inserts=is_bulk_inserts,
            )
        return artists_loaded + labels_loaded

    @classmethod
    def insert_bulk(cls, bulk_inserts, inserted_count):
        worker = WorkerEntityInserter(
            bulk_inserts=bulk_inserts,
            inserted_count=inserted_count,
        )
        return worker

    @classmethod
    def update_bulk(cls, bulk_updates, processed_count):
        worker = WorkerEntityUpdater(
            bulk_updates=bulk_updates,
            processed_count=processed_count,
        )
        return worker

    @classmethod
    @timeit
    def loader_pass_two(cls) -> None:
        log.debug("entity loader pass two")
        cls.loader_start_workers(WorkerEntityPassTwo)

    @classmethod
    @timeit
    def loader_pass_three(cls):
        log.debug("entity loader pass three")
        cls.loader_start_workers(WorkerEntityPassThree)

    @classmethod
    def loader_start_workers(cls, worker_class) -> None:
        number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE)

        with transaction():
            entity_repository = EntityRepository()
            total_count = entity_repository.count()
            entity_type: EntityType = EntityType.ARTIST
            batched_release_ids = entity_repository.get_batched_ids(
                entity_type, number_in_batch
            )

        current_total = 0

        workers = []
        for release_ids in batched_release_ids:
            worker = worker_class(entity_type, release_ids, current_total, total_count)
            worker.start()
            workers.append(worker)
            current_total += number_in_batch

            if len(workers) > get_concurrency_count():
                cls.loader_process_worker(workers)

        while len(workers) > 0:
            cls.loader_process_worker(workers)

        with transaction():
            entity_repository = EntityRepository()
            total_count = entity_repository.count()
            entity_type: EntityType = EntityType.LABEL
            batched_release_ids = entity_repository.get_batched_ids(
                entity_type, number_in_batch
            )

        current_total = 0

        workers = []
        for release_ids in batched_release_ids:
            worker = worker_class(entity_type, release_ids, current_total, total_count)
            worker.start()
            workers.append(worker)
            current_total += number_in_batch

            if len(workers) > get_concurrency_count():
                cls.loader_process_worker(workers)

        while len(workers) > 0:
            cls.loader_process_worker(workers)

    @classmethod
    def loader_process_worker(cls, workers) -> None:
        worker = workers.pop(0)
        if LOGGING_TRACE:
            log.debug(f"wait for worker {worker.name} - {len(workers)} left in list")
        worker.join()
        if worker.exitcode > 0:
            log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
            raise Exception("Error in worker process")
        worker.terminate()

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
