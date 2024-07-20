import logging

from discograph.database import get_concurrency_count
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader.worker_relation_pass_one import WorkerRelationPassOne
from discograph.library.loader.worker_relation_pass_two import WorkerRelationPassTwo
from discograph.utils import timeit

log = logging.getLogger(__name__)


class LoaderRelation(LoaderBase):
    # PUBLIC METHODS

    @classmethod
    @timeit
    def loader_relation_pass_one(cls, date: str):
        log.debug(f"relation loader pass one - date: {date}")

        with transaction():
            release_repository = ReleaseRepository()
            total_count = release_repository.count()
            if total_count > LoaderBase.BULK_INSERT_BATCH_SIZE * 10:
                number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE)
            else:
                number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE / 10)

            batched_release_ids = release_repository.get_batched_ids(number_in_batch)

        current_total = 0

        workers = []
        for release_ids in batched_release_ids:
            worker = WorkerRelationPassOne(release_ids, current_total, total_count)
            worker.start()
            workers.append(worker)
            current_total += number_in_batch

            if len(workers) > get_concurrency_count():
                worker = workers.pop(0)
                cls.loader_wait_for_worker(worker)

        while len(workers) > 0:
            worker = workers.pop(0)
            cls.loader_wait_for_worker(worker)

    @classmethod
    @timeit
    def loader_relation_pass_two(cls, date: str):
        log.debug(f"relation loader pass two - date: {date}")

        with transaction():
            release_repository = ReleaseRepository()
            total_count = release_repository.count()
            if total_count > LoaderBase.BULK_INSERT_BATCH_SIZE * 10:
                number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE)
            else:
                number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE / 10)

            batched_release_ids = release_repository.get_batched_ids(number_in_batch)

        current_total = 0

        workers = []
        for release_ids in batched_release_ids:
            worker = WorkerRelationPassTwo(release_ids, current_total, total_count)
            worker.start()
            workers.append(worker)
            current_total += number_in_batch

            if len(workers) > get_concurrency_count():
                worker = workers.pop(0)
                cls.loader_wait_for_worker(worker)

        while len(workers) > 0:
            worker = workers.pop(0)
            cls.loader_wait_for_worker(worker)

    @classmethod
    def insert_bulk(cls, bulk_inserts, inserted_count):
        pass

    @classmethod
    def update_bulk(cls, bulk_updates, processed_count):
        pass

    @classmethod
    def delete_bulk(cls, bulk_deletes, processed_count):
        pass

    @classmethod
    def get_set_of_ids(cls, entity_type):
        pass

    @classmethod
    @timeit
    def loader_vacuum(
        cls, has_tablename: bool, is_full: bool, is_analyze: bool
    ) -> None:
        log.debug(f"relation loader vacuum")
        with transaction():
            relation_repository = RelationRepository()
            relation_repository.vacuum(has_tablename, is_full, is_analyze)
