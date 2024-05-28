import logging

from discograph.database import get_concurrency_count
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader.worker_relation_pass_one import WorkerRelationPassOne
from discograph.logging_config import LOGGING_TRACE
from discograph.utils import timeit

log = logging.getLogger(__name__)


class LoaderRelation:
    # PUBLIC METHODS

    @classmethod
    @timeit
    def loader_relation_pass_one(cls, date: str):
        log.debug("relation loader pass one")

        with transaction():
            release_repository = ReleaseRepository()
            total_count = release_repository.count()
            if total_count > LoaderBase.BULK_INSERT_BATCH_SIZE * 10:
                number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE)
            else:
                number_in_batch = int(LoaderBase.BULK_INSERT_BATCH_SIZE / 100)

            batched_release_ids = release_repository.get_batched_release_ids(
                number_in_batch
            )

        current_total = 0

        workers = []
        for release_ids in batched_release_ids:
            worker = WorkerRelationPassOne(release_ids, current_total, total_count)
            worker.start()
            workers.append(worker)
            current_total += number_in_batch

            if len(workers) > get_concurrency_count() * 2:
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
