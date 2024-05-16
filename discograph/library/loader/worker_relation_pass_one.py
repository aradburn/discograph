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
            max_attempts = LoaderBase.MAX_RETRYS
            error = True
            while error and max_attempts != 0:
                error = False
                with transaction():
                    relation_repository = RelationRepository()
                    release_repository = ReleaseRepository()

                    try:
                        self.loader_pass_one_inner(
                            relation_repository=relation_repository,
                            release_repository=release_repository,
                            release_id=release_id,
                            proc_name=proc_name,
                        )
                        count += 1
                        if count % LoaderBase.BULK_REPORTING_SIZE == 0:
                            log.debug(
                                f"[{proc_name}] processed {count} of {self.total_count}"
                            )
                    except DatabaseError:
                        log.debug(
                            f"Database record locked in updating relation {max_attempts} goes left"
                        )
                        max_attempts -= 1
                        error = True
                        relation_repository.rollback()
                        utils.sleep_with_backoff(LoaderBase.MAX_RETRYS - max_attempts)
                    except OperationalError as e:
                        log.exception(e)

            if error:
                log.debug(f"Error in updating relations for release: {release_id}")

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    @classmethod
    def loader_pass_one_inner(
        cls,
        *,
        relation_repository: RelationRepository,
        release_repository: ReleaseRepository,
        release_id,
        proc_name="",
    ):
        try:
            # log.debug(f"loader_pass_one ({annotation}): {release_id}")
            release = release_repository.get(release_id)
            relations = RelationDataAccess.from_release(release)
            if LOGGING_TRACE:
                log.debug(
                    f"Relation (Pass 1) [{proc_name}]\t"
                    + f"(release_id:{release.release_id})\t[{len(relations)}] {release.title}"
                )
            for relation_dict in relations:
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

        except NotFoundError:
            log.debug(
                f"Relation loader_pass_one_inner release_id not found: {release_id}"
            )

    @classmethod
    def create_relation(
        cls,
        *,
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

            # save, ignore if already exists
            relation_db = relation_repository.create(relation_uncommitted)

            relation_repository.commit()

        return relation_db

    @classmethod
    def create_relation_old(
        cls,
        *,
        relation_repository: RelationRepository,
        relation_dict: dict,
    ) -> Relation:
        try:
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

            # save, ignore if already exists
            relation_db = relation_repository.create(relation_uncommitted)

            relation_repository.commit()
        except DatabaseError:
            relation_repository.rollback()

            key = {
                "entity_one_id": relation_dict["entity_one_id"],
                "entity_one_type": relation_dict["entity_one_type"],
                "entity_two_id": relation_dict["entity_two_id"],
                "entity_two_type": relation_dict["entity_two_type"],
                "role_name": RoleDataAccess.normalize(relation_dict["role"]),
            }
            relation_db = relation_repository.find_by_key(key)

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
            relation_repository.update(relation.relation_id, {"releases": updated})
            relation_repository.commit()
