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
            batched_release_ids = release_repository.get_batched_release_ids(
                int(LoaderBase.BULK_INSERT_BATCH_SIZE / 100)
            )

        workers = []
        for release_ids in batched_release_ids:
            worker = WorkerRelationPassOne(release_ids)
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
