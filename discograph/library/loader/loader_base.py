import gzip
import logging
import pprint
from abc import abstractmethod
from random import random
from typing import List, Generator, Self

from sqlalchemy.exc import DataError

from discograph.database import get_concurrency_count
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader_utils import LoaderUtils
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class LoaderBase:
    BULK_INSERT_BATCH_SIZE = 10000
    BULK_UPDATE_BATCH_SIZE = 1000
    BULK_REPORTING_SIZE = 10000
    MAX_RETRYS = 100
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
        ids: List[int] = []

        initial_count = repository.count()

        processed_count = 0
        xml_path = LoaderUtils.get_xml_path(data_directory, xml_tag, date)
        log.info(f"Loading data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            bulk_records = []
            workers = []
            for i, element in enumerate(iterator):
                data = None
                try:
                    data = cls.tags_to_fields(element)
                    if skip_without:
                        if any(not data.get(_) for _ in skip_without):
                            continue
                    if element.get("id"):
                        data[id_attr] = element.get("id")
                    ids.append(int(data[id_attr]))
                    data["random"] = random()
                    # log.debug(f"data: {data}")

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
                            if LOGGING_TRACE:
                                log.debug(f"wait for worker {len(workers)} in list")
                            worker.join()
                            if worker.exitcode > 0:
                                log.debug(
                                    f"worker {worker.name} exitcode: {worker.exitcode}"
                                )
                                raise Exception("Error in worker process")
                            worker.terminate()

                except DataError as e:
                    log.exception("Error in loader_pass_one", pprint.pformat(data))
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
                if LOGGING_TRACE:
                    log.debug(
                        f"wait for worker {worker.name} - {len(workers)} left in list"
                    )
                worker.join()
                if worker.exitcode > 0:
                    log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                    raise Exception("Error in worker process")
                worker.terminate()

            repository_count = repository.count()
            log.debug(f"repository_count: {repository_count}")

            new_inserts_count = repository_count - initial_count
            log.debug(f"processed_count: {processed_count}")
            log.debug(f"new_inserts_count: {new_inserts_count}")
            # assert processed_count == repository_count

            # Check if any records have been deleted
            number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE)
            log.debug(f"got ids: {len(ids)}")

            if xml_tag == "artist":
                log.debug(f"check artist ids: {len(ids)}")
                entity_type: EntityType = EntityType.ARTIST
                entity_repository = EntityRepository()
                batched_entity_ids = entity_repository.get_batched_ids(
                    entity_type, number_in_batch
                )
                for entity_ids in batched_entity_ids:
                    for entity_id in entity_ids:
                        if entity_id not in ids:
                            log.debug(f"Found deleted artist id: {entity_id}")
                            entity_repository.delete_by_id(entity_id, entity_type)
                            entity_repository.commit()
            elif xml_tag == "label":
                log.debug(f"check label ids: {len(ids)}")
                entity_type: EntityType = EntityType.LABEL
                entity_repository = EntityRepository()
                batched_entity_ids = entity_repository.get_batched_ids(
                    entity_type, number_in_batch
                )
                for entity_ids in batched_entity_ids:
                    for entity_id in entity_ids:
                        if entity_id not in ids:
                            log.debug(f"Found deleted label id: {entity_id}")
                            entity_repository.delete_by_id(entity_id, entity_type)
                            entity_repository.commit()
            elif xml_tag == "release":
                log.debug(f"check release ids: {len(ids)}")
                release_repository = ReleaseRepository()
                batched_release_ids = release_repository.get_batched_release_ids(
                    number_in_batch
                )
                for release_ids in batched_release_ids:
                    for release_id in release_ids:
                        if release_id not in ids:
                            log.debug(f"Found deleted release id: {release_id}")
                            release_repository.delete_by_id(release_id)
                            release_repository.commit()
        return processed_count

    @classmethod
    @abstractmethod
    def insert_bulk(cls, bulk_inserts, processed_count):
        pass

    @classmethod
    @abstractmethod
    def update_bulk(cls, bulk_updates, processed_count):
        pass

    @classmethod
    def load_from_xml(
        cls,
        domain_class,
        data_directory: str,
        date: str,
        xml_tag: str,
        id_attr: str,
        skip_without: List[str],
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
    def preprocess_data(cls, data: dict, element) -> dict:
        return data

    @classmethod
    def tags_to_fields(cls, element, ignore_none=None, mapping=None) -> dict:
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
