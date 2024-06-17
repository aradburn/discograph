import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.transaction import transaction

log = logging.getLogger(__name__)


class WorkerBulkInserter(multiprocessing.Process):
    def __init__(
        self,
        repository: BaseRepository,
        bulk_inserts,
        inserted_count,
    ):
        super().__init__()
        self.repository = repository
        self.bulk_inserts = bulk_inserts
        self.inserted_count = inserted_count

    def run(self):
        worker_process_name = (
            self.name + "(" + self.repository.schema_class.__name__ + ")"
        )

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()
            self.repository.__init__()

        with transaction():
            try:
                self.repository.save_all(self.bulk_inserts)
                self.repository.commit()
            except DatabaseError as e:
                log.exception("Error in WorkerBulkInserter worker", e)
                raise e
        log.info(f"[{worker_process_name}] inserted_count: {self.inserted_count}")
