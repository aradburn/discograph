import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.entity_table import EntityTable
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.transaction import transaction
from discograph.library.fields.entity_type import EntityType
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerEntityPassThree(multiprocessing.Process):
    def __init__(self, entity_type: EntityType, entity_ids):
        super().__init__()
        self.entity_type = entity_type
        self.entity_ids = entity_ids

    def run(self):
        proc_name = self.name
        # entity_class_name = self.model_class.__qualname__
        # entity_module_name = self.model_class.__module__
        # relation_class_name = entity_class_name.replace("EntityDB", "RelationDB")
        # relation_module_name = entity_module_name.replace("entitydb", "relationdb")
        # relation_class = getattr(sys.modules[relation_module_name], relation_class_name)

        count = 0
        total_count = len(self.entity_ids)

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for i, entity_id in enumerate(self.entity_ids):
            with transaction():
                entity_repository = EntityRepository()
                relation_repository = RelationRepository()
                try:
                    self.loader_pass_three_single(
                        entity_repository,
                        relation_repository,
                        entity_id=entity_id,
                        entity_type=self.entity_type,
                    )
                    count += 1
                    if count % 1000 == 0:
                        log.info(f"[{proc_name}] processed {count} of {total_count}")
                except DatabaseError:
                    log.exception(
                        f"ERROR: {entity_id}-{self.entity_type} in process {proc_name}"
                    )
        # session.close()

        log.info(f"[{proc_name}] processed {count} of {total_count}")

    @staticmethod
    def loader_pass_three_single(
        entity_repository: EntityRepository,
        relation_repository: RelationRepository,
        entity_id: int,
        entity_type: EntityType,
    ):
        _relation_counts = {}

        # Get all relations for this entity, where the entity is the first part of the relation
        relations = relation_repository.find_by_entity_one_key(entity_id, entity_type)
        # log.debug(f"relations count: {len(relations)}")

        for relation in relations:
            # log.debug(f"relation: {relation}")
            if relation.role not in _relation_counts:
                _relation_counts[relation.role] = set()
            key = (
                relation.entity_one_id,
                relation.entity_one_type,
                relation.entity_two_id,
                relation.entity_two_type,
            )
            _relation_counts[relation.role].add(key)

        # Get all relations for this entity, where the entity is the second part of the relation
        relations = relation_repository.find_by_entity_two_key(entity_id, entity_type)
        # log.debug(f"relations count: {len(relations)}")

        for relation in relations:
            # log.debug(f"relation: {relation}")
            if relation.role not in _relation_counts:
                _relation_counts[relation.role] = set()
            key = (
                relation.entity_one_id,
                relation.entity_one_type,
                relation.entity_two_id,
                relation.entity_two_type,
            )
            _relation_counts[relation.role].add(key)

        for role, keys in _relation_counts.items():
            _relation_counts[role] = len(keys)

        # if not _relation_counts:
        #     return

        entity_repository.update(
            entity_id, entity_type, {EntityTable.relation_counts.key: _relation_counts}
        )
        # Update the relation counts for this entity
        # pk = (entity_id, entity_type)
        # entity = entity_repository.get(pk)
        #
        # entity.relation_counts = _relation_counts
        # flag_modified(entity, cls.relation_counts.key)
        if LOGGING_TRACE:
            log.debug(
                f"Entity (Pass 3)\t"
                + f"{entity_id}-{entity_type}: {len(_relation_counts)}"
            )
        entity_repository.commit()
