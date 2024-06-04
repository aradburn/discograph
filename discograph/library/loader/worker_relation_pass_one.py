import logging
import multiprocessing
import pprint
from random import random

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
from discograph.library.domain.relation import RelationUncommitted, Relation
from discograph.library.loader.loader_base import LoaderBase
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerRelationPassOne(multiprocessing.Process):
    def __init__(self, release_ids, current_total: int, total_count: int):
        super().__init__()
        self.release_ids = release_ids
        self.current_total = current_total
        self.total_count = total_count

    def run(self):
        proc_name = self.name

        count = self.current_total

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for release_id in self.release_ids:

            with transaction():
                release_repository = ReleaseRepository()

                try:
                    # log.debug(f"loader_pass_one ({annotation}): {release_id}")
                    release = release_repository.get(release_id)
                    relations = RelationDataAccess.from_release(release)
                    if LOGGING_TRACE:
                        log.debug(
                            f"Relation (Pass 1) [{proc_name}]\t"
                            + f"(release_id:{release.release_id})\t[{len(relations)}] {release.title}"
                        )
                except NotFoundError:
                    log.debug(
                        f"Relation loader_pass_one_inner release_id not found: {release_id}"
                    )

            for relation_dict in relations:
                max_attempts = LoaderBase.MAX_RETRYS
                error = True
                while error and max_attempts != 0:
                    error = False

                    with transaction():
                        relation_repository = RelationRepository()

                        try:
                            self.loader_pass_one_inner(
                                relation_repository=relation_repository,
                                release_id=release_id,
                                relation_dict=relation_dict,
                            )
                            relation_repository.commit()
                        except DatabaseError:
                            relation_repository.rollback()

                            max_attempts -= 1
                            error = True
                            utils.sleep_with_backoff(
                                LoaderBase.MAX_RETRYS - max_attempts
                            )
                        except OperationalError as e:
                            log.exception(e)

                if error:
                    log.debug(f"Error in updating relations for release: {release_id}")

            count += 1
            if count % LoaderBase.BULK_REPORTING_SIZE == 0:
                log.debug(f"[{proc_name}] processed {count} of {self.total_count}")

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    @classmethod
    def loader_pass_one_inner(
        cls,
        relation_repository: RelationRepository,
        release_id: int,
        relation_dict,
    ):
        # log.debug(f"  loader_pass_one ({annotation}): {relation}")
        relation_db = cls.create_relation(
            relation_repository=relation_repository,
            relation_dict=relation_dict,
        )
        year = relation_dict.get("year")
        cls.update_relation(
            relation_repository=relation_repository,
            relation=relation_db,
            release_id=release_id,
            year=year,
        )

    @classmethod
    def create_relation(
        cls,
        relation_repository: RelationRepository,
        relation_dict: dict,
    ) -> Relation:
        try:
            key = {
                "entity_one_id": relation_dict["entity_one_id"],
                "entity_one_type": relation_dict["entity_one_type"],
                "entity_two_id": relation_dict["entity_two_id"],
                "entity_two_type": relation_dict["entity_two_type"],
                "role_name": RoleDataAccess.normalize(relation_dict["role"]),
            }
            relation_db = relation_repository.find_by_key(key)
        except NotFoundError:
            relation_db = None

        if relation_db is None:
            relation_uncommitted = RelationUncommitted(
                entity_one_id=relation_dict["entity_one_id"],
                entity_one_type=relation_dict["entity_one_type"],
                entity_two_id=relation_dict["entity_two_id"],
                entity_two_type=relation_dict["entity_two_type"],
                role_name=RoleDataAccess.normalize(relation_dict["role"]),
                releases={},
                random=random(),
            )
            # log.debug(f"new relation: {new_instance}")

            # save, throw error if already exists
            relation_db = relation_repository.create(relation_uncommitted)

            relation_repository.commit()
        # except NotFoundError:
        #     print(f"NotFound")
        #     relation_db = None

        return relation_db

    @classmethod
    def update_relation(
        cls,
        relation_repository: RelationRepository,
        relation: Relation,
        release_id: int,
        year: int,
    ) -> None:
        original = relation.releases.copy()
        updated = relation.releases.copy()
        updated[str(release_id)] = year

        differences = DeepDiff(
            original,
            updated,
        )
        diff = pprint.pformat(differences)
        if diff != "{}":
            # log.debug(f"diff: {diff}")
            relation_repository.update(
                relation.relation_id,
                relation.version_id,
                {"releases": updated, "version_id": relation.version_id + 1},
            )
            relation_repository.commit()
