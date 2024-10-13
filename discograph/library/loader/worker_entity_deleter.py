import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.transaction import transaction

log = logging.getLogger(__name__)


class WorkerEntityDeleter(multiprocessing.Process):
    def __init__(
        self,
        bulk_deletes: list[int],
        processed_count: int,
    ):
        super().__init__()
        self.bulk_deletes = bulk_deletes
        self.processed_count = processed_count

    def run(self):
        proc_name = self.name
        deleted_count = 0

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for id_ in self.bulk_deletes:
            with transaction():
                entity_repository = EntityRepository()
                relation_repository = RelationRepository()
                try:
                    relation_repository.delete_by_entitys(id_)
                    entity_repository.delete_by_id(id_)

                    deleted_count += 1
                except DatabaseError as e:
                    log.exception("Error in WorkerEntityDeleter worker", exc_info=True)
                    raise e

        log.info(
            f"[{proc_name}] processed: {self.processed_count}, deleted: {deleted_count}"
        )
