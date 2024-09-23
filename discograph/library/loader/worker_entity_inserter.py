import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.transaction import transaction

log = logging.getLogger(__name__)


class WorkerEntityInserter(multiprocessing.Process):
    def __init__(
        self,
        bulk_inserts,
        inserted_count,
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
            except DatabaseError as e:
                log.exception("Error in WorkerEntityInserter worker", exc_info=True)
                raise e
        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")
