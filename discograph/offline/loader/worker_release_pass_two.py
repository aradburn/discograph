import logging
import multiprocessing

from discograph.exceptions import DatabaseError
from discograph.offline.data_access_layer.entity_data_access import EntityDataAccess
from discograph.offline.database.offline_database_helper import OfflineDatabaseHelper
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.database.release_repository import ReleaseRepository
from discograph.offline.database.release_table import ReleaseTable
from discograph.offline.database.transaction import transaction
from discograph.offline.loader.loader_base import LoaderBase
from discograph.logging_config import LOGGING_TRACE
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


class WorkerReleasePassTwo(multiprocessing.Process):
    def __init__(self, release_ids: list[int], current_total: int, total_count: int):
        super().__init__()
        self.release_ids = release_ids
        self.current_total = current_total
        self.total_count = total_count

    def run(self):
        proc_name = self.name

        count = self.current_total
        end_count = count + len(self.release_ids)

        if OfflineDatabaseManager.get_concurrency_count() > 1:
            OfflineDatabaseHelper.initialize()

        for id_ in self.release_ids:
            with transaction():
                entity_repository = EntityRepository()
                release_repository = ReleaseRepository()
                try:
                    self.loader_pass_two_single(
                        entity_repository=entity_repository,
                        release_repository=release_repository,
                        id_=id_,
                        annotation=proc_name,
                    )
                except DatabaseError:
                    log.error("Error in WorkerReleasePassTwo worker")
                    # log.exception("Error in WorkerReleasePassTwo worker", exc_info=True)
                    raise

            count += 1
            if count % LoaderBase.BULK_REPORTING_SIZE == 0 and not count == end_count:
                log.debug(f"[{proc_name}] processed {count} of {self.total_count}")

        log.info(f"[{proc_name}] processed {count} of {self.total_count}")

    @staticmethod
    def loader_pass_two_single(
        *,
        entity_repository: EntityRepository,
        release_repository: ReleaseRepository,
        id_,
        annotation="",
    ):
        release = release_repository.get(id_)
        # log.debug(f"artists       before: {release.artists}")
        # log.debug(f"tracklist before: {release.tracklist}")
        # log.debug(f"labels        before: {release.labels}")
        # log.debug(f"companies     before: {release.companies}")
        changed = EntityDataAccess.resolve_release_references(
            entity_repository, release
        )
        if changed:
            if LOGGING_TRACE:
                log.debug(
                    f"Release (Pass 2) [{annotation}]\t"
                    + f"          (id:{release.release_id}): {release.title}"
                )
            # log.debug(f"artists        after: {release.artists}")
            # log.debug(f"tracklist  after: {release.tracklist}")
            # log.debug(f"labels         after: {release.labels}")
            # log.debug(f"companies      after: {release.companies}")
            release_repository.update(
                id_,
                {
                    # ReleaseTable.artists.key: release.artists,
                    # ReleaseTable.extra_artists.key: release.extra_artists,
                    ReleaseTable.labels.key: release.labels,
                    ReleaseTable.companies.key: release.companies,
                    # ReleaseTable.tracklist.key: release.tracklist,
                },
            )
            release_repository.commit()
        elif LOGGING_TRACE:
            log.debug(
                f"Release (Pass 2) [{annotation}]\t"
                + f"[SKIPPED] (id:{release.release_id}): {release.title}"
            )
