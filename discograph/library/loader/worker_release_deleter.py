import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction

log = logging.getLogger(__name__)


class WorkerReleaseDeleter(multiprocessing.Process):
    def __init__(
        self,
        bulk_deletes,
        processed_count,
    ):
        super().__init__()
        self.bulk_deletes = bulk_deletes
        self.processed_count = processed_count

    def run(self):
        proc_name = self.name
        deleted_count = 0

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for release_id, _ in self.bulk_deletes:
            with transaction():
                release_repository = ReleaseRepository()
                try:
                    release_repository.delete_by_id(release_id)
                    deleted_count += 1
                except DatabaseError as e:
                    log.exception("Database Error in WorkerReleaseDeleter worker", e)
                    raise e

        log.info(
            f"[{proc_name}] processed: {self.processed_count}, deleted: {deleted_count}"
        )
