import logging
import multiprocessing
from random import random
from typing import List

from sqlalchemy.exc import OperationalError, IntegrityError

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
        total_count = count + len(self.release_ids)

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        with transaction():
            release_repository = ReleaseRepository()
            relation_repository = RelationRepository()

            for release_id in self.release_ids:

                try:
                    # log.debug(f"loader_pass_one ({annotation}): {release_id}")
                    release = release_repository.get(release_id)
                    relation_dicts = RelationDataAccess.from_release(release)
                    if LOGGING_TRACE:
                        log.debug(
                            f"WorkerRelationPassOne [{proc_name}]\t"
                            + f"(release_id:{release.release_id})\t[{len(relations)}] {release.title}"
                        )
                except NotFoundError:
                    log.debug(
                        f"WorkerRelationPassOne release_id not found: {release_id}"
                    )
                except DatabaseError as e:
                    log.error("Database Error in WorkerRelationPassOne")
                    # log.exception("Database Error in WorkerRelationPassOne", e)
                    raise e

                relations = self.to_relations_from_dict(relation_dicts)

                if len(relations) > 0:
                    self.create_relation_bulk(
                        relation_repository,
                        relations,
                    )

                count += 1
                if (
                    count % LoaderBase.BULK_REPORTING_SIZE == 0
                    and not count == total_count
                ):
                    log.debug(f"[{proc_name}] processed {count} of {self.total_count}")

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    @classmethod
    def create_relation_bulk(
        cls,
        relation_repository: RelationRepository,
        relations: List[RelationUncommitted],
    ) -> None:

        # save, throw error if already exists
        try:
            relation_repository.create_bulk(relations, on_conflict_do_nothing=True)
            relation_repository.commit()
            # log.debug(f"inserted {len(relations)}")
        except DatabaseError:
            relation_repository.rollback()
            # log.debug(f"Roll back create")
            # log.exception(e)
            relation_db = None
        except IntegrityError:
            relation_repository.rollback()
            # log.debug(f"Roll back create")
            relation_db = None
        except OperationalError as e:
            relation_repository.rollback()
            log.debug(f"Record is locked in create")
            raise e

    @classmethod
    def to_relations_from_dict(
        cls, relation_dicts: List[dict]
    ) -> List[RelationUncommitted]:
        relation_uncommitteds = []
        for relation_dict in relation_dicts:
            relation_uncommitted = RelationUncommitted(
                entity_one_id=relation_dict["entity_one_id"],
                entity_one_type=relation_dict["entity_one_type"],
                entity_two_id=relation_dict["entity_two_id"],
                entity_two_type=relation_dict["entity_two_type"],
                role_name=RoleDataAccess.normalize(relation_dict["role"]),
                releases={},
                random=random(),
            )
            relation_uncommitteds.append(relation_uncommitted)
        return relation_uncommitteds

    # def run(self):
    #     proc_name = self.name
    #
    #     count = self.current_total
    #     total_count = count + len(self.release_ids)
    #
    #     if get_concurrency_count() > 1:
    #         DatabaseHelper.initialize()
    #
    #     with transaction():
    #         release_repository = ReleaseRepository()
    #         relation_repository = RelationRepository()
    #
    #         for release_id in self.release_ids:
    #
    #             try:
    #                 # log.debug(f"loader_pass_one ({annotation}): {release_id}")
    #                 release = release_repository.get(release_id)
    #                 relations = RelationDataAccess.from_release(release)
    #                 if LOGGING_TRACE:
    #                     log.debug(
    #                         f"WorkerRelationPassOne [{proc_name}]\t"
    #                         + f"(release_id:{release.release_id})\t[{len(relations)}] {release.title}"
    #                     )
    #             except NotFoundError:
    #                 log.debug(
    #                     f"WorkerRelationPassOne release_id not found: {release_id}"
    #                 )
    #             except DatabaseError as e:
    #                 log.error("Database Error in WorkerRelationPassOne")
    #                 # log.exception("Database Error in WorkerRelationPassOne", e)
    #                 raise e
    #
    #             for relation_dict in relations:
    #                 max_attempts = LoaderBase.MAX_RETRYS
    #                 error = True
    #                 while error and max_attempts != 0:
    #                     error = False
    #
    #                     try:
    #                         self.create_relation(
    #                             relation_repository=relation_repository,
    #                             relation_dict=relation_dict,
    #                         )
    #                         relation_repository.commit()
    #                     except DatabaseError as e:
    #                         relation_repository.rollback()
    #
    #                         if (
    #                                 LOGGING_TRACE
    #                                 or max_attempts < LoaderBase.MAX_RETRYS - 1
    #                         ):
    #                             log.error(e)
    #
    #                         max_attempts -= 1
    #                         error = True
    #                         utils.sleep_with_backoff(
    #                             LoaderBase.MAX_RETRYS - max_attempts
    #                         )
    #                     except OperationalError as e:
    #                         relation_repository.rollback()
    #
    #                         if LOGGING_TRACE or max_attempts <= 1:
    #                             log.exception(
    #                                 f"Database OperationalError in WorkerRelationPassOne (create)",
    #                                 e,
    #                             )
    #
    #                         max_attempts -= 1
    #                         error = True
    #                         utils.sleep_with_backoff(
    #                             LoaderBase.MAX_RETRYS - max_attempts
    #                         )
    #
    #                 if error:
    #                     log.error(
    #                         f"Error in updating relations for release: {release_id}"
    #                     )
    #                     raise Exception(
    #                         f"Error in updating relations for release: {release_id}"
    #                     )
    #
    #             count += 1
    #             if (
    #                     count % LoaderBase.BULK_REPORTING_SIZE == 0
    #                     and not count == total_count
    #             ):
    #                 log.debug(f"[{proc_name}] processed {count} of {self.total_count}")
    #
    #     log.info(f"[{proc_name}] processed {count} of {self.total_count}")
    #
    # @classmethod
    # def create_relation(
    #         cls,
    #         relation_repository: RelationRepository,
    #         relation_dict: dict,
    # ) -> Relation:
    #
    #     relation_uncommitted = RelationUncommitted(
    #         entity_one_id=relation_dict["entity_one_id"],
    #         entity_one_type=relation_dict["entity_one_type"],
    #         entity_two_id=relation_dict["entity_two_id"],
    #         entity_two_type=relation_dict["entity_two_type"],
    #         role_name=RoleDataAccess.normalize(relation_dict["role"]),
    #         releases={},
    #         random=random(),
    #     )
    #     # log.debug(f"new relation: {new_instance}")
    #
    #     # save, throw error if already exists
    #     try:
    #         relation_db = relation_repository.create(
    #             relation_uncommitted, on_conflict_do_nothing=True
    #         )
    #         relation_repository.commit()
    #     except DatabaseError:
    #         relation_repository.rollback()
    #         # log.debug(f"Roll back create")
    #         # log.exception(e)
    #         relation_db = None
    #     except IntegrityError:
    #         relation_repository.rollback()
    #         # log.debug(f"Roll back create")
    #         relation_db = None
    #     except OperationalError as e:
    #         relation_repository.rollback()
    #         log.debug(f"Record is locked in create")
    #         raise e
    #
    #     return relation_db
