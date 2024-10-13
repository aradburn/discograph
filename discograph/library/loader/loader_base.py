import gzip
import logging
from abc import abstractmethod
from random import random
from typing import List, Generator, Self, Any

from sortedcontainers import SortedSet
from sqlalchemy.exc import DataError

from discograph import utils
from discograph.database import get_concurrency_count
from discograph.library.database.base_repository import BaseRepository
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader.loader_utils import LoaderUtils
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class LoaderBase:
    BULK_INSERT_BATCH_SIZE = 1000
    BULK_UPDATE_BATCH_SIZE = 100
    BULK_REPORTING_SIZE = 1000
    # BULK_INSERT_BATCH_SIZE = 10000
    # BULK_UPDATE_BATCH_SIZE = 1000
    # BULK_REPORTING_SIZE = 10000
    MAX_RETRYS = 10
    _tags_to_fields_mapping: dict = None

    @classmethod
    def loader_pass_one_manager(
        cls,
        repository: BaseRepository,
        data_directory: str,
        date: str,
        xml_tag: str,
        id_attr: str,
        skip_without: List[str],
        is_bulk_inserts=False,
    ) -> int:
        # Loader pass one.
        set_of_updated_ids: SortedSet[int] = SortedSet()

        initial_count = repository.count()

        processed_count = 0
        xml_path = LoaderUtils.get_xml_path(data_directory, xml_tag, date)
        log.info(f"Loading data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            bulk_records = []
            workers = []
            for i, element in enumerate(iterator):
                try:
                    data = cls.tags_to_fields(element)
                    if skip_without:
                        if any(not data.get(_) for _ in skip_without):
                            continue
                    # if element.get("id"):
                    #     data[id_attr] = element.get("id")
                    # data["random"] = random()
                    # log.debug(f"data: {data}")

                    set_of_updated_ids.add(int(data[id_attr]))

                    bulk_records.append(data)
                    processed_count += 1

                    if get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_records) >= LoaderBase.BULK_INSERT_BATCH_SIZE:
                            if is_bulk_inserts:
                                worker = cls.insert_bulk(
                                    bulk_records,
                                    processed_count,
                                )
                            else:
                                worker = cls.update_bulk(
                                    bulk_records,
                                    processed_count,
                                )
                            worker.start()
                            workers.append(worker)
                            bulk_records.clear()
                        if len(workers) > get_concurrency_count():
                            worker = workers.pop(0)
                            cls.loader_wait_for_worker(worker)

                except DataError as e:
                    log.exception("Error in loader_pass_one", exc_info=True)
                    raise e

            if len(bulk_records) > 0:
                if is_bulk_inserts:
                    worker = cls.insert_bulk(
                        bulk_records,
                        processed_count,
                    )
                else:
                    worker = cls.update_bulk(
                        bulk_records,
                        processed_count,
                    )
                worker.start()
                workers.append(worker)
                bulk_records.clear()

            while len(workers) > 0:
                worker = workers.pop(0)
                cls.loader_wait_for_worker(worker)

            repository_count = repository.count()
            log.debug(f"repository_count: {repository_count}")

            new_inserts_count = repository_count - initial_count
            log.debug(f"processed_count: {processed_count}")
            log.debug(f"new_inserts_count: {new_inserts_count}")

            if xml_tag == "artist":
                entity_type = EntityType.ARTIST
            elif xml_tag == "label":
                entity_type = EntityType.LABEL
            elif xml_tag == "release":
                entity_type = None
            set_of_database_ids = cls.get_set_of_ids(entity_type)

            # Check if any records need to be deleted
            # (present in database and not present in the xml dump)
            ids_to_be_deleted = set_of_database_ids - set_of_updated_ids

            log.debug(f"number of update ids  : {len(set_of_updated_ids)}")
            log.debug(f"number of database ids: {len(set_of_database_ids)}")
            log.debug(f"number to be deleted  : {len(ids_to_be_deleted)}")

            number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE / 10)

            if len(ids_to_be_deleted) > 0:
                workers = []
                batched_ids_to_be_deleted = utils.batched(
                    ids_to_be_deleted, number_in_batch
                )

                for batch_of_ids_to_be_deleted in batched_ids_to_be_deleted:
                    worker = cls.delete_bulk(
                        batch_of_ids_to_be_deleted,
                        len(batch_of_ids_to_be_deleted),
                    )
                    worker.start()
                    workers.append(worker)

                    if len(workers) > get_concurrency_count():
                        worker = workers.pop(0)
                        cls.loader_wait_for_worker(worker)

                while len(workers) > 0:
                    worker = workers.pop(0)
                    cls.loader_wait_for_worker(worker)

        return processed_count

    @classmethod
    @abstractmethod
    def insert_bulk(cls, bulk_inserts: list[dict[str, Any]], processed_count: int):
        pass

    @classmethod
    @abstractmethod
    def update_bulk(cls, bulk_updates: list[dict[str, Any]], processed_count: int):
        pass

    @classmethod
    @abstractmethod
    def delete_bulk(cls, bulk_deletes: list[int], processed_count: int):
        pass

    @classmethod
    @abstractmethod
    def get_set_of_ids(cls, entity_type):
        pass

    @classmethod
    def loader_wait_for_worker(cls, worker) -> None:
        if LOGGING_TRACE:
            log.debug(f"wait for worker {worker.name}")
        worker.join()
        worker.terminate()
        if worker.exitcode > 0:
            log.error(f"worker {worker.name} exitcode: {worker.exitcode}")
            raise RuntimeError("Error in worker process")

    @classmethod
    def load_from_xml(
        cls,
        domain_class,
        data_directory: str,
        date: str,
        xml_tag: str,
        id_attr: str,
        skip_without: list[str],
    ) -> Generator[Self, None, None]:
        xml_path = LoaderUtils.get_xml_path(data_directory, xml_tag, date)
        log.info(f"Loading data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            for i, element in enumerate(iterator):
                data = cls.tags_to_fields(element)
                if skip_without:
                    if any(not data.get(_) for _ in skip_without):
                        continue
                if element.get("id"):
                    data[id_attr] = element.get("id")
                data["random"] = random()
                # log.debug(f"data: {data}")

                new_instance = domain_class(**data)
                # log.debug(f"new_instance: {new_instance}")
                yield new_instance

    @classmethod
    def from_element(cls, element) -> Self:
        pass

    @classmethod
    def preprocess_data(cls, data: dict, element) -> dict[str, Any]:
        return data

    @classmethod
    def tags_to_fields(cls, element, ignore_none=None, mapping=None) -> dict[str, Any]:
        data = {}
        mapping = mapping or cls._tags_to_fields_mapping
        for child_element in element:
            entry = mapping.get(child_element.tag, None)
            if entry is None:
                continue
            field_name, procedure = entry
            value = procedure(child_element)
            if ignore_none and value is None:
                continue
            data[field_name] = value
        data = cls.preprocess_data(data, element)
        return data
