import logging
import multiprocessing
from typing import Any

from sqlalchemy.exc import DatabaseError

from discograph.runtime.runtime_database.runtime_database_helper import (
    RuntimeDatabaseHelper,
)
from discograph.runtime.runtime_database.runtime_entity_repository import (
    RuntimeEntityRepository,
)
from discograph.runtime.runtime_database.runtime_transaction import runtime_transaction
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

log = logging.getLogger(__name__)


class TransferWorkerEntityInserter(multiprocessing.Process):
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

        if RuntimeDatabaseManager.get_concurrency_count() > 1:
            RuntimeDatabaseHelper.initialize()

        with runtime_transaction():
            runtime_entity_repository = RuntimeEntityRepository()
            try:
                runtime_entity_repository.save_all(self.bulk_inserts)
                runtime_entity_repository.commit()
            except DatabaseError:
                log.error("Error in TransferWorkerEntityInserter worker")
                # log.exception("Error in TransferWorkerEntityInserter worker", exc_info=True)
                raise
        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")
