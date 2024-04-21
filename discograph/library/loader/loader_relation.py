import logging

from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction
from discograph.library.loader.worker_relation_pass_one import WorkerRelationPassOne
from discograph.utils import timeit

log = logging.getLogger(__name__)


class LoaderRelation:
    # PUBLIC METHODS

    @classmethod
    @timeit
    def loader_pass_one(cls, date: str):
        log.debug("relation loader pass one")
        with transaction():
            release_repository = ReleaseRepository()
            # relation_class_name = cls.__qualname__
            # relation_module_name = cls.__module__
            # release_class_name = relation_class_name.replace("RelationDB", "ReleaseDB")
            # release_module_name = relation_module_name.replace(
            #     "relationdb", "releasedb"
            # )
            # release_class = getattr(
            #     sys.modules[release_module_name], release_class_name
            # )

            chunked_release_ids = release_repository.get_chunked_release_ids()
            workers = [
                WorkerRelationPassOne(release_ids)
                for release_ids in chunked_release_ids
            ]
            log.debug(f"relation loader pass one - start {len(workers)} workers")
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
                if worker.exitcode > 0:
                    log.error(
                        f"relation loader worker {worker.name} exitcode: {worker.exitcode}"
                    )
                    raise Exception("Error in worker process")
            for worker in workers:
                worker.terminate()
