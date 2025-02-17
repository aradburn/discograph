import logging
import multiprocessing
from typing import Any

from sqlalchemy.exc import DatabaseError

from discograph.offline.database.offline_database_helper import OfflineDatabaseHelper
from discograph.offline.database.release_repository import ReleaseRepository
from discograph.offline.database.transaction import transaction
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


class WorkerReleaseInserter(multiprocessing.Process):
    def __init__(
        self,
        bulk_inserts: list[dict[str, Any]],
        inserted_count,
    ):
        super().__init__()
        self.bulk_inserts = bulk_inserts
        self.inserted_count = inserted_count

    def run(self):
        proc_name = self.name

        if OfflineDatabaseManager.get_concurrency_count() > 1:
            OfflineDatabaseHelper.initialize()

        with transaction():
            release_repository = ReleaseRepository()
            try:
                release_repository.save_all(self.bulk_inserts)
                release_repository.commit()
            except DatabaseError as e:
                log.exception(
                    "Database Error in WorkerReleaseInserter worker", exc_info=True
                )
                raise e

        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")
