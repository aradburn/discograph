import logging
import multiprocessing
import pprint
from typing import Any

from deepdiff import DeepDiff

from discograph.database import get_concurrency_count
from discograph.exceptions import DatabaseError, NotFoundError
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.release_table import ReleaseTable
from discograph.library.database.transaction import transaction
from discograph.library.domain.release import Release
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerReleaseUpdater(multiprocessing.Process):
    def __init__(self, bulk_updates: list[dict[str, Any]], processed_count: int):
        super().__init__()
        self.bulk_updates = bulk_updates
        self.processed_count = processed_count

    def run(self):
        proc_name = self.name
        updated_count = 0
        inserted_count = 0

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for data in self.bulk_updates:
            with transaction():
                release_repository = ReleaseRepository()
                updated_release = Release(**data)
                try:
                    db_release = release_repository.get(updated_release.release_id)

                    differences = DeepDiff(
                        db_release,
                        updated_release,
                        exclude_paths=[
                            "id",
                            "random",
                            "dirty_fields",
                            "_dirty",
                            "root.labels[0]['id']",
                            "root.labels[1]['id']",
                            "root.labels[2]['id']",
                            "root.labels[3]['id']",
                            "root.labels[4]['id']",
                            "root.labels[5]['id']",
                            "root.labels[6]['id']",
                            "root.labels[7]['id']",
                            "root.labels[8]['id']",
                            "root.labels[9]['id']",
                            "root.labels[10]['id']",
                            "root.labels[11]['id']",
                            "root.labels[12]['id']",
                            "root.labels[13]['id']",
                            "root.labels[14]['id']",
                            "root.labels[15]['id']",
                            "root.labels[16]['id']",
                            "root.labels[17]['id']",
                            "root.labels[18]['id']",
                            "root.labels[19]['id']",
                            "root.labels[20]['id']",
                        ],
                        ignore_numeric_type_changes=True,
                    )
                    diff = pprint.pformat(differences)
                    if diff != "{}":
                        if LOGGING_TRACE:
                            log.debug(f"diff: {diff}")
                        # log.debug(f"old_release: {old_release}")
                        # log.debug(f"updated_release: {updated_release}")

                        # differences2 = DeepDiff(
                        #     old_entity.entities,
                        #     updated_entity.entities,
                        #     ignore_numeric_type_changes=True,
                        # )
                        # diff2 = pprint.pformat(differences2)
                        # if diff2 != "{}":
                        #     log.debug(f"entities diff: {diff2}")
                        # Update release
                        release_repository.update(
                            db_release.release_id,
                            {
                                ReleaseTable.release_id.key: updated_release.release_id,
                                ReleaseTable.artists.key: updated_release.artists,
                                ReleaseTable.companies.key: updated_release.companies,
                                ReleaseTable.country.key: updated_release.country,
                                ReleaseTable.extra_artists.key: updated_release.extra_artists,
                                ReleaseTable.formats.key: updated_release.formats,
                                ReleaseTable.genres.key: updated_release.genres,
                                ReleaseTable.identifiers.key: updated_release.identifiers,
                                ReleaseTable.labels.key: updated_release.labels,
                                ReleaseTable.master_id.key: updated_release.master_id,
                                ReleaseTable.notes.key: updated_release.notes,
                                ReleaseTable.release_date.key: updated_release.release_date,
                                ReleaseTable.styles.key: updated_release.styles,
                                ReleaseTable.title.key: updated_release.title,
                                ReleaseTable.tracklist.key: updated_release.tracklist,
                            },
                        )

                        release_repository.commit()
                        updated_count += 1
                except NotFoundError:
                    if LOGGING_TRACE:
                        log.debug(
                            f"New insert in WorkerReleaseUpdater: {updated_release.release_id}"
                        )
                    try:
                        release_repository.create(updated_release)
                        release_repository.commit()
                        inserted_count += 1
                    except DatabaseError as e:
                        log.exception("Database Error in WorkerReleaseUpdater worker")
                        raise e
                except DatabaseError as e:
                    log.exception("Database Error in WorkerReleaseUpdater")
                    raise e

        log.info(
            f"[{proc_name}] processed_count: {self.processed_count}, "
            + f"updated: {updated_count}, inserted: {inserted_count}"
        )
