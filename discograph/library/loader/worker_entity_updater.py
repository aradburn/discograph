import logging
import multiprocessing
import pprint

from deepdiff import DeepDiff

from discograph.database import get_concurrency_count
from discograph.exceptions import DatabaseError, NotFoundError
from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.entity_table import EntityTable
from discograph.library.database.transaction import transaction
from discograph.library.domain.entity import Entity
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerEntityUpdater(multiprocessing.Process):
    def __init__(self, bulk_updates, processed_count):
        super().__init__()
        self.bulk_updates = bulk_updates
        self.processed_count = processed_count

    def run(self):
        proc_name = self.name
        updated_count = 0
        inserted_count = 0

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for i, data in enumerate(self.bulk_updates):
            with transaction():
                entity_repository = EntityRepository()
                updated_entity = Entity(**data)
                try:
                    if LOGGING_TRACE:
                        log.debug(
                            f"update: {updated_entity.entity_id}-{updated_entity.entity_type}"
                        )

                    db_entity = entity_repository.get(
                        updated_entity.entity_id, updated_entity.entity_type
                    )

                    is_changed = False
                    update_payload = {}

                    if db_entity.entity_name != updated_entity.entity_name:
                        # Update name
                        db_entity.entity_name = updated_entity.entity_name
                        update_payload[EntityTable.entity_name.key] = (
                            db_entity.entity_name
                        )

                        # Update search_content
                        db_entity.search_content = (
                            EntityDataAccess.normalise_search_content(
                                updated_entity.entity_name
                            )
                        )
                        update_payload[EntityTable.search_content.key] = (
                            db_entity.search_content
                        )
                        is_changed = True

                    # Update metadata
                    differences = DeepDiff(
                        db_entity,
                        updated_entity,
                        include_paths=[
                            "entity_metadata",
                        ],
                        ignore_numeric_type_changes=True,
                    )
                    diff = pprint.pformat(differences)
                    if diff != "{}":
                        if LOGGING_TRACE:
                            log.debug(f"diff: {diff}")
                        # log.debug(f"db_entity     : {db_entity}")
                        # log.debug(f"updated_entity: {updated_entity}")

                        db_entity.entity_metadata = updated_entity.entity_metadata

                        update_payload[EntityTable.entity_metadata.key] = (
                            db_entity.entity_metadata
                        )
                        is_changed = True

                    if is_changed:
                        entity_repository.update(
                            db_entity.entity_id, db_entity.entity_type, update_payload
                        )
                        entity_repository.commit()
                        updated_count += 1
                except NotFoundError:
                    # log.debug(
                    #     f"New insert in WorkerEntityUpdater: {updated_entity.entity_id}-{updated_entity.entity_type}"
                    # )
                    try:
                        entity_repository.create(updated_entity)
                        entity_repository.commit()
                        inserted_count += 1
                    except DatabaseError as e:
                        log.exception("Error in WorkerEntityUpdater worker")
                        raise e
                except DatabaseError as e:
                    log.exception("Error in WorkerEntityUpdater", e)
                    raise e

        log.info(
            f"[{proc_name}] processed_count: {self.processed_count}, "
            + f"updated: {updated_count}, inserted: {inserted_count}"
        )
