import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.entity_table import EntityTable
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.transaction import transaction
from discograph.library.loader.loader_base import LoaderBase

log = logging.getLogger(__name__)


class WorkerEntityPassThree(multiprocessing.Process):
    def __init__(self, ids: list[int], current_total: int, total_count: int):
        super().__init__()
        self.ids = ids
        self.current_total = current_total
        self.total_count = total_count

    def run(self):
        proc_name = self.name

        count = self.current_total
        end_count = count + len(self.ids)

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for id_ in self.ids:
            with transaction():
                entity_repository = EntityRepository()
                relation_repository = RelationRepository()
                try:
                    self.loader_pass_three_single(
                        entity_repository,
                        relation_repository,
                        id_=id_,
                    )

                except DatabaseError as e:
                    log.exception(
                        f"Database Error for entity id: {id_} in process {proc_name}",
                        exc_info=True,
                    )
                    raise e

            count += 1
            if count % LoaderBase.BULK_REPORTING_SIZE == 0 and not count == end_count:
                log.debug(f"[{proc_name}] processed {count} of {self.total_count}")

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    @staticmethod
    def loader_pass_three_single(
        entity_repository: EntityRepository,
        relation_repository: RelationRepository,
        id_: int,
    ):
        _relation_counts = {}

        # Get all relations for this entity, where the entity is the subject or object of the relation
        # log.debug(f"id_: {id_}")
        relations = relation_repository.find_by_entity(id_)
        # log.debug(f"relations count: {len(relations)}")

        for relation in relations:
            # log.debug(f"relation: {relation}")
            if relation.role not in _relation_counts:
                _relation_counts[relation.role] = set()
            key = (
                relation.subject,
                relation.object,
            )
            _relation_counts[relation.role].add(key)

        for role, keys in _relation_counts.items():
            _relation_counts[role] = len(keys)

        try:
            # Update the relation counts for this entity
            entity_repository.update(
                id_, {EntityTable.relation_counts.key: _relation_counts}
            )

            entity_repository.commit()
        except DatabaseError as e:
            log.exception(
                f"Database Error for id: {id_}",
                exc_info=True,
            )
            raise e
