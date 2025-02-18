import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.offline.database.offline_database_helper import OfflineDatabaseHelper
from discograph.offline.database.release_repository import ReleaseRepository
from discograph.offline.database.offline_transaction import offline_transaction
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


class WorkerReleaseDeleter(multiprocessing.Process):
    def __init__(self, bulk_deletes: list[int], processed_count: int):
        super().__init__()
        self.bulk_deletes = bulk_deletes
        self.processed_count = processed_count

    def run(self):
        proc_name = self.name
        deleted_count = 0

        if OfflineDatabaseManager.get_concurrency_count() > 1:
            OfflineDatabaseHelper.initialize()

        for id_ in self.bulk_deletes:
            with offline_transaction():
                release_repository = ReleaseRepository()
                try:
                    release_repository.delete_by_id(id_)
                    deleted_count += 1
                except DatabaseError as e:
                    log.exception(
                        "Database Error in WorkerReleaseDeleter worker", exc_info=True
                    )
                    raise e

        log.info(
            f"[{proc_name}] processed: {self.processed_count}, deleted: {deleted_count}"
        )
