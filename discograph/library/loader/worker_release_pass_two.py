import logging
import multiprocessing

from discograph.database import get_concurrency_count
from discograph.exceptions import DatabaseError
from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.release_table import ReleaseTable
from discograph.library.database.transaction import transaction
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerReleasePassTwo(multiprocessing.Process):
    def __init__(self, release_ids):
        super().__init__()
        self.release_ids = release_ids

    def run(self):
        proc_name = self.name
        corpus = {}
        count = 0
        total_count = len(self.release_ids)

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for i, release_id in enumerate(self.release_ids):
            with transaction():
                entity_repository = EntityRepository()
                release_repository = ReleaseRepository()
                progress = float(i) / total_count
                try:
                    self.loader_pass_two_single(
                        entity_repository=entity_repository,
                        release_repository=release_repository,
                        release_id=release_id,
                        annotation=proc_name,
                        corpus=corpus,
                        progress=progress,
                    )
                    count += 1
                except DatabaseError:
                    log.exception("ERROR:", release_id, proc_name)

        log.info(f"[{proc_name}] processed {count} of {total_count}")

    @staticmethod
    def loader_pass_two_single(
        *,
        entity_repository: EntityRepository,
        release_repository: ReleaseRepository,
        release_id,
        annotation="",
        corpus=None,
        progress=None,
    ):
        release = release_repository.get(release_id)
        corpus = corpus or {}
        changed = EntityDataAccess().resolve_references_from_release(
            entity_repository, release, corpus=corpus
        )
        if changed:
            if LOGGING_TRACE:
                log.debug(
                    f"Release (Pass 2) {progress:.1%} [{annotation}]\t"
                    + f"          (id:{release.release_id}): {release.title}"
                )
            release_repository.update(
                release_id, {ReleaseTable.labels.key: release.labels}
            )
            # flag_modified(release, cls.labels.key)
            release_repository.commit()
        elif LOGGING_TRACE:
            log.debug(
                f"Release (Pass 2) {progress:.1%} [{annotation}]\t"
                + f"[SKIPPED] (id:{release.release_id}): {release.title}"
            )
