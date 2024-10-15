import logging
import multiprocessing
from typing import Any

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.transaction import transaction

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

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        with transaction():
            entity_repository = EntityRepository()
            try:
                entity_repository.save_all(self.bulk_inserts)
                entity_repository.commit()
            except DatabaseError:
                log.error("Error in WorkerEntityInserter worker")
                # log.exception("Error in WorkerEntityInserter worker", exc_info=True)
                raise
        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")
