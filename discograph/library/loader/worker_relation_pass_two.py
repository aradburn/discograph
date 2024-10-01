import logging
import multiprocessing
from typing import List

from sqlalchemy.exc import OperationalError

from discograph.database import get_concurrency_count
from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.data_access_layer.relation_data_access import RelationDataAccess
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.relation_release_year_repository import (
    RelationReleaseYearRepository,
)
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.relation_release_year import (
    RelationReleaseYearUncommitted,
)
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

        relation_release_years = []

        count = self.current_total
        end_count = count + len(self.release_ids)

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
                    log.exception(
                        "Database Error in WorkerRelationPassTwo (getting relations from release)"
                    )
                    raise e

            for relation_dict in relations:

                with transaction():
                    relation_repository = RelationRepository()
                    relation_release_year_repository = RelationReleaseYearRepository()

                    year = relation_dict.get("year")
                    new_relation_release_years = self.to_relation_release_years(
                        relation_repository=relation_repository,
                        relation_dict=relation_dict,
                        release_id=release_id,
                        year=year,
                    )
                    relation_release_years.extend(new_relation_release_years)

                    if len(relation_release_years) > LoaderBase.BULK_INSERT_BATCH_SIZE:
                        self.create_relation_release_year_bulk(
                            relation_release_year_repository, relation_release_years
                        )
                        relation_release_years.clear()

            count += 1
            if count % LoaderBase.BULK_REPORTING_SIZE == 0 and not count == end_count:
                log.debug(f"[{proc_name}] processed {count} of {self.total_count}")

        if len(relation_release_years) > 0:
            with transaction():
                relation_release_year_repository = RelationReleaseYearRepository()
                self.create_relation_release_year_bulk(
                    relation_release_year_repository, relation_release_years
                )

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    @classmethod
    def to_relation_release_years(
        cls,
        relation_repository: RelationRepository,
        relation_dict: dict,
        release_id: int,
        year: int,
    ) -> List[RelationReleaseYearUncommitted]:
        relation_release_years = []
        role_names = RoleDataAccess.normalise_role_names(relation_dict["role"])
        for role_name in role_names:
            try:
                key = {
                    "entity_one_id": relation_dict["entity_one_id"],
                    "entity_one_type": relation_dict["entity_one_type"],
                    "entity_two_id": relation_dict["entity_two_id"],
                    "entity_two_type": relation_dict["entity_two_type"],
                    "role_name": role_name,
                }
                relation_id = relation_repository.get_id_by_key(key)
                # log.debug(f"v: {relation_db.version_id}")
                relation_release_year_uncommitted = RelationReleaseYearUncommitted(
                    relation_id=relation_id,
                    release_id=release_id,
                    year=year,
                )
                relation_release_years.append(relation_release_year_uncommitted)
            except NotFoundError:
                relation_repository.rollback()
                log.debug(f"Error cannot find relation")
            except DatabaseError:
                relation_repository.rollback()
                log.debug(f"Error cannot find relation")
            except OperationalError as e:
                relation_repository.rollback()
                raise e
        return relation_release_years

    @classmethod
    def create_relation_release_year_bulk(
        cls,
        relation_release_year_repository: RelationReleaseYearRepository,
        relation_release_years: List[RelationReleaseYearUncommitted],
    ) -> None:
        try:
            relation_release_year_repository.create_bulk(relation_release_years)
            relation_release_year_repository.commit()
            # log.debug(
            #     f"Created RelationReleaseYear: {relation_db.relation_id} {release_id} {year}"
            # )
        except DatabaseError:
            relation_release_year_repository.rollback()
            log.debug(f"Error cannot create RelationReleaseYear")
        except OperationalError as e:
            relation_release_year_repository.rollback()
            raise e
