import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction

log = logging.getLogger(__name__)


class WorkerReleaseInserter(multiprocessing.Process):
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
            release_repository = ReleaseRepository()
            try:
                release_repository.save_all(self.bulk_inserts)
                release_repository.commit()
            except DatabaseError as e:
                log.exception("Database Error in WorkerReleaseInserter worker", e)
                raise e
        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")
