import logging
import multiprocessing
from typing import Any

from sqlalchemy.exc import DatabaseError

from discograph.runtime.runtime_database.runtime_database_helper import (
    RuntimeDatabaseHelper,
)
from discograph.runtime.runtime_database.runtime_relation_repository import (
    RuntimeRelationRepository,
)
from discograph.runtime.runtime_database.runtime_transaction import runtime_transaction
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

log = logging.getLogger(__name__)


class TransferWorkerRelationInserter(multiprocessing.Process):
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
            runtime_relation_repository = RuntimeRelationRepository()
            try:
                runtime_relation_repository.save_all(self.bulk_inserts)
                runtime_relation_repository.commit()
            except DatabaseError:
                log.error("Error in TransferWorkerRelationInserter worker")
                # log.exception("Error in TransferWorkerRelationInserter worker", exc_info=True)
                raise
        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")
