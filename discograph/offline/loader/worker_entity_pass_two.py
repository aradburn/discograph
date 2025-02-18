import logging
import multiprocessing

from discograph.exceptions import NotFoundError, DatabaseError
from discograph.offline.data_access_layer.entity_data_access import EntityDataAccess
from discograph.offline.database.offline_database_helper import OfflineDatabaseHelper
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.database.entity_table import EntityTable
from discograph.offline.database.offline_transaction import offline_transaction
from discograph.offline.domain.entity import Entity
from discograph.offline.loader.loader_base import LoaderBase
from discograph.logging_config import LOGGING_TRACE
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


class WorkerEntityPassTwo(multiprocessing.Process):
    def __init__(self, ids: list[int], current_total: int, total_count: int):
        super().__init__()
        self.ids = ids
        self.current_total = current_total
        self.total_count = total_count

    def run(self):
        proc_name = self.name

        count = self.current_total
        end_count = count + len(self.ids)

        if OfflineDatabaseManager.get_concurrency_count() > 1:
            OfflineDatabaseHelper.initialize()

        for id_ in self.ids:
            max_attempts = 10
            error = True
            while error and max_attempts != 0:
                error = False
                with offline_transaction():
                    entity_repository = EntityRepository()
                    try:
                        entity = entity_repository.get_by_id(id_)
                        self.worker_pass_two_single(
                            entity_repository=entity_repository,
                            entity=entity,
                            annotation=proc_name,
                        )
                    except NotFoundError:
                        log.warning(
                            f"Database NotFoundError: {entity.entity_id}-{entity.entity_type} in process: {proc_name}"
                        )
                        entity_repository.rollback()
                        max_attempts -= 1
                        error = True
                    except DatabaseError as e:
                        log.exception(
                            f"Database Error for entity_id: {entity.entity_id}-{entity.entity_type} "
                            + f"in process: {proc_name}",
                            exc_info=True,
                        )
                        raise e

            if error:
                log.debug(
                    f"Error in updating references for entity_id: {entity.entity_id}"
                )
                raise Exception(
                    f"Error in updating references for entity_id: {entity.entity_id}"
                )

            count += 1
            if count % LoaderBase.BULK_REPORTING_SIZE == 0 and not count == end_count:
                log.debug(f"[{proc_name}] processed {count} of {self.total_count}")

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    # PUBLIC METHODS

    @staticmethod
    def worker_pass_two_single(
        *,
        entity_repository: EntityRepository,
        entity: Entity,
        annotation="",
    ):
        if LOGGING_TRACE:
            log.debug(f"id: {entity.entity_id}-{entity.entity_type}")

        changed = EntityDataAccess.resolve_entity_references(entity_repository, entity)
        if changed:
            if LOGGING_TRACE:
                log.debug(
                    f"Entity (Pass 2) [{annotation}]\t"
                    + f"          (id: {entity.entity_id}-{entity.entity_type}): {entity.entity_name}"
                )
            entity_repository.update(
                entity.id,
                {EntityTable.entities.key: entity.entities},
            )
            entity_repository.commit()

    # @classmethod
    # def set_search_content(cls, session: Session):
    #     for entity in cls.get_entity_iterator(session, entity_type=EntityType.ARTIST):
    #         with session.begin():
    #             entity.search_content = cls.string_to_tsvector(entity.entity_name)
    #             # document.save()
    #             log.debug(
    #                 f"search_content ({entity.entity_type}:{entity.entity_id}):\t"
    #                 + f"{entity.entity_name} -> {entity.search_content}"
    #             )
    #     for entity in cls.get_entity_iterator(session, entity_type=EntityType.LABEL):
    #         with session.begin():
    #             entity.search_content = cls.string_to_tsvector(entity.entity_name)
    #             # document.save()
    #             log.debug(
    #                 f"search_content ({entity.entity_type}:{entity.entity_id}):\t"
    #                 + f"{entity.entity_name} -> {entity.search_content}"
    #             )
