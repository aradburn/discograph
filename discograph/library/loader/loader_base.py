import gzip
import logging
import pprint
from random import random
from typing import List, Generator, Self

from sqlalchemy.exc import DataError

from discograph.database import get_concurrency_count
from discograph.library.database.base_repository import BaseRepository
from discograph.library.loader.worker_pass_one import WorkerPassOne
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
        date: str,
        xml_tag: str,
        id_attr: str,
        skip_without: List[str],
    ) -> int:
        # Loader pass one.

        initial_count = repository.count()
        inserted_count = 0
        xml_path = LoaderUtils.get_xml_path(xml_tag, date)
        log.info(f"Loading data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            bulk_inserts = []
            bulk_release_genre_inserts = []
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
                    data["random"] = random()
                    # log.debug(f"data: {data}")

                    bulk_inserts.append(data)
                    inserted_count += 1
                    if get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_inserts) >= LoaderBase.BULK_INSERT_BATCH_SIZE:
                            worker = cls.insert_bulk(
                                repository,
                                bulk_inserts,
                                inserted_count,
                            )
                            worker.start()
                            workers.append(worker)
                            bulk_inserts.clear()
                            bulk_release_genre_inserts.clear()
                        if len(workers) > get_concurrency_count() * 2:
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

            if len(bulk_inserts) > 0:
                repository.save_all(bulk_inserts)

            final_count = repository.count()
            updated_count = final_count - initial_count
            log.debug(f"inserted_count: {inserted_count}")
            log.debug(f"updated_count: {updated_count}")
            assert inserted_count == updated_count
        return inserted_count

    @classmethod
    def insert_bulk(cls, repository, bulk_inserts, inserted_count):
        worker = WorkerPassOne(
            repository=repository,
            bulk_inserts=bulk_inserts,
            inserted_count=inserted_count,
        )
        return worker

    @classmethod
    def load_from_xml(
        cls,
        domain_class,
        date: str,
        xml_tag: str,
        id_attr: str,
        skip_without: List[str],
    ) -> Generator[Self, None, None]:
        xml_path = LoaderUtils.get_xml_path(xml_tag, date)
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
