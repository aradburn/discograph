import logging
import multiprocessing
import pprint

from deepdiff import DeepDiff
from sqlalchemy.exc import OperationalError

from discograph import utils
from discograph.database import get_concurrency_count
from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.data_access_layer.relation_data_access import RelationDataAccess
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction
from discograph.library.loader.loader_base import LoaderBase
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerRelationPassTwo(multiprocessing.Process):
    def __init__(self, release_ids, current_total: int, total_count: int):
        super().__init__()
        self.release_ids = release_ids
        self.current_total = current_total
        self.total_count = total_count

    def run(self):
        proc_name = self.name

        count = self.current_total
        total_count = count + len(self.release_ids)

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for release_id in self.release_ids:

            with transaction():
                release_repository = ReleaseRepository()

                try:
                    release = release_repository.get(release_id)
                    relations = RelationDataAccess.from_release(release)
                    if LOGGING_TRACE:
                        log.debug(
                            f"WorkerRelationPassTwo [{proc_name}]\t"
                            + f"(release_id:{release.release_id})\t[{len(relations)}] {release.title}"
                        )
                except NotFoundError:
                    log.debug(
                        f"WorkerRelationPassTwo release_id not found: {release_id}"
                    )
                except DatabaseError as e:
                    log.error("Database Error in WorkerRelationPassTwo")
                    # log.exception("Database Error in WorkerRelationPassTwo", e)
                    raise e

            for relation_dict in relations:
                max_attempts = LoaderBase.MAX_RETRYS
                error = True
                while error and max_attempts != 0:
                    error = False

                    with transaction():
                        relation_repository = RelationRepository()

                        try:
                            year = relation_dict.get("year")
                            self.update_relation(
                                relation_repository=relation_repository,
                                relation_dict=relation_dict,
                                release_id=release_id,
                                year=year,
                            )
                            relation_repository.commit()
                        except DatabaseError as e:
                            relation_repository.rollback()

                            if (
                                LOGGING_TRACE
                                or max_attempts < LoaderBase.MAX_RETRYS - 1
                            ):
                                log.exception(e)

                            max_attempts -= 1
                            error = True
                            utils.sleep_with_backoff(
                                LoaderBase.MAX_RETRYS - max_attempts
                            )
                        except OperationalError as e:
                            relation_repository.rollback()

                            if LOGGING_TRACE or max_attempts <= 1:
                                log.exception(
                                    f"Database OperationalError in WorkerRelationPassTwo (update)",
                                    e,
                                )
                            max_attempts -= 1
                            error = True
                            utils.sleep_with_backoff(
                                LoaderBase.MAX_RETRYS - max_attempts
                            )

                if error:
                    log.error(f"Error in updating relations for release: {release_id}")
                    raise Exception(
                        f"Error in updating relations for release: {release_id}"
                    )

            count += 1
            if count % LoaderBase.BULK_REPORTING_SIZE == 0 and not count == total_count:
                log.debug(f"[{proc_name}] processed {count} of {self.total_count}")

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    @classmethod
    def update_relation(
        cls,
        relation_repository: RelationRepository,
        relation_dict: dict,
        release_id: int,
        year: int,
    ) -> None:
        retry_wanted = True
        while retry_wanted:
            try:
                key = {
                    "entity_one_id": relation_dict["entity_one_id"],
                    "entity_one_type": relation_dict["entity_one_type"],
                    "entity_two_id": relation_dict["entity_two_id"],
                    "entity_two_type": relation_dict["entity_two_type"],
                    "role_name": RoleDataAccess.normalize(relation_dict["role"]),
                }
                relation_db = relation_repository.find_by_key(key)
                # log.debug(f"v: {relation_db.version_id}")
            except DatabaseError:
                relation_repository.rollback()
                log.debug(f"Error cannot find relation")
                relation_db = None
            except OperationalError as e:
                relation_repository.rollback()
                # log.debug(f"Record is locked")
                raise e

            if relation_db is not None:
                original = relation_db.releases.copy()
                updated = relation_db.releases.copy()
                updated[str(release_id)] = year

                differences = DeepDiff(
                    original,
                    updated,
                )
                diff = pprint.pformat(differences)
                if diff != "{}":
                    # log.debug(f"diff: {diff}")
                    try:
                        relation_repository.update_one(
                            relation_db.relation_id,
                            relation_db.version_id,
                            {
                                "releases": updated,
                                "version_id": relation_db.version_id + 1,
                            },
                        )
                        relation_repository.commit()
                        retry_wanted = False
                    except DatabaseError:
                        relation_repository.rollback()
                else:
                    retry_wanted = False
