import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.offline.database.offline_database_helper import OfflineDatabaseHelper
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.database.relation_repository import RelationRepository
from discograph.offline.database.transaction import transaction
from discograph.offline.offline_database_manager import OfflineDatabaseManager

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

        if OfflineDatabaseManager.get_concurrency_count() > 1:
            OfflineDatabaseHelper.initialize()

        for id_ in self.bulk_deletes:
            with transaction():
                entity_repository = EntityRepository()
                relation_repository = RelationRepository()
                try:
                    relation_repository.delete_by_entitys(id_)
                    entity_repository.delete_by_id(id_)

                    deleted_count += 1
                except DatabaseError:
                    log.error("Error in WorkerEntityDeleter worker")
                    # log.exception("Error in WorkerEntityDeleter worker", exc_info=True)
                    raise

        log.info(
            f"[{proc_name}] processed: {self.processed_count}, deleted: {deleted_count}"
        )
