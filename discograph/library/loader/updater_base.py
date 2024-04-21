import gzip
import logging
import pprint
from abc import abstractmethod
from random import random
from typing import List

from discograph.database import get_concurrency_count
from discograph.exceptions import DatabaseError
from discograph.library.database.base_repository import BaseRepository
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader_utils import LoaderUtils

log = logging.getLogger(__name__)


class UpdaterBase(LoaderBase):
    @classmethod
    def updater_pass_one_manager(
        cls,
        repository: BaseRepository,
        date: str,
        xml_tag: str,
        id_attr: str,
        name_attr: str,
        skip_without: List[str],
    ):
        # Updater pass one.
        # initial_count = len(model_class)
        processed_count = 0
        xml_path = LoaderUtils.get_xml_path(xml_tag, date)
        log.info(f"Loading update data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            bulk_updates = []
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
                    # log.debug(f"{data}")
                    bulk_updates.append(data)
                    # updated_entity = model_class(**data)
                    # bulk_updates.append(updated_entity)
                    processed_count += 1
                    # log.debug(f"updated_entity: {updated_entity}")
                    if get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_updates) >= LoaderBase.BULK_UPDATE_BATCH_SIZE:
                            worker = cls.update_bulk(bulk_updates, processed_count)
                            worker.start()
                            workers.append(worker)
                            bulk_updates.clear()
                        if len(workers) > get_concurrency_count():
                            worker = workers.pop(0)
                            # log.debug(f"wait for worker {len(workers)} in list")
                            worker.join()
                            if worker.exitcode > 0:
                                log.error(
                                    f"worker {worker.name} exitcode: {worker.exitcode}"
                                )
                                # raise Exception("Error in worker process")
                            worker.terminate()
                except DatabaseError as e:
                    log.exception("Error in updater_pass_one", pprint.pformat(data))
                    raise e

            if len(bulk_updates) > 0:
                worker = cls.update_bulk(bulk_updates, processed_count)
                worker.start()
                workers.append(worker)
                bulk_updates.clear()

            while len(workers) > 0:
                worker = workers.pop(0)
                worker.join()
                if worker.exitcode > 0:
                    log.error(f"worker {worker.name} exitcode: {worker.exitcode}")
                    # raise Exception("Error in worker process")
                worker.terminate()

    @classmethod
    @abstractmethod
    def update_bulk(cls, bulk_updates, processed_count):
        pass
