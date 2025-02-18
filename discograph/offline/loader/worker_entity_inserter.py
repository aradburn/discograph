import logging
import multiprocessing
from typing import Any

from sqlalchemy.exc import DatabaseError

from discograph.offline.database.offline_database_helper import OfflineDatabaseHelper
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.database.offline_transaction import offline_transaction
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


class WorkerEntityInserter(multiprocessing.Process):
    def __init__(
        self,
        bulk_inserts: list[dict[str, Any]],
        inserted_count: int,
    ):
        super().__init__()
        self.bulk_inserts = bulk_inserts
        self.inserted_count = inserted_count

    def run(self):
        proc_name = self.name

        if OfflineDatabaseManager.get_concurrency_count() > 1:
            OfflineDatabaseHelper.initialize()

        with offline_transaction():
            entity_repository = EntityRepository()
            try:
                entity_repository.save_all(self.bulk_inserts)
                entity_repository.commit()
            except DatabaseError:
                log.error("Error in WorkerEntityInserter worker")
                # log.exception("Error in WorkerEntityInserter worker", exc_info=True)
                raise
        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")
