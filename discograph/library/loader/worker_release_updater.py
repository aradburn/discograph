import logging
import multiprocessing
import pprint

from deepdiff import DeepDiff

from discograph.database import get_concurrency_count
from discograph.exceptions import DatabaseError
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.release_table import ReleaseTable
from discograph.library.database.transaction import transaction
from discograph.library.domain.release import Release
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerReleaseUpdater(multiprocessing.Process):
    def __init__(self, bulk_updates, processed_count):
        super().__init__()
        self.bulk_updates = bulk_updates
        self.processed_count = processed_count

    def run(self):
        proc_name = self.name
        updated_count = 0

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for i, data in enumerate(self.bulk_updates):
            with transaction():
                repository = ReleaseRepository()
                updated_release = Release(**data)
                try:
                    db_release = repository.get(updated_release.release_id)

                    differences = DeepDiff(
                        db_release,
                        updated_release,
                        exclude_paths=[
                            "release_id",
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
                        repository.update(
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
                        # db_release.artists = updated_release.artists
                        # db_release.companies = updated_release.companies
                        # db_release.country = updated_release.country
                        # db_release.extra_artists = updated_release.extra_artists
                        # db_release.formats = updated_release.formats
                        # db_release.genres = updated_release.genres
                        # db_release.identifiers = updated_release.identifiers
                        # db_release.labels = updated_release.labels
                        # db_release.master_id = updated_release.master_id
                        # db_release.notes = updated_release.notes
                        # db_release.release_date = updated_release.release_date
                        # db_release.styles = updated_release.styles
                        # db_release.title = updated_release.title
                        # db_release.tracklist = updated_release.tracklist
                        # flag_modified(db_release, self.model_class.artists.key)
                        # flag_modified(db_release, self.model_class.companies.key)
                        # flag_modified(db_release, self.model_class.country.key)
                        # flag_modified(db_release, self.model_class.extra_artists.key)
                        # flag_modified(db_release, self.model_class.formats.key)
                        # flag_modified(db_release, self.model_class.genres.key)
                        # flag_modified(db_release, self.model_class.identifiers.key)
                        # flag_modified(db_release, self.model_class.labels.key)
                        # flag_modified(db_release, self.model_class.master_id.key)
                        # flag_modified(db_release, self.model_class.notes.key)
                        # flag_modified(db_release, self.model_class.release_date.key)
                        # flag_modified(db_release, self.model_class.styles.key)
                        # flag_modified(db_release, self.model_class.title.key)
                        # flag_modified(db_release, self.model_class.tracklist.key)
                        # q = self.model_class.update(
                        #     {
                        #         self.model_class.release_id: updated_release.release_id,
                        #         self.model_class.artists: updated_release.artists,
                        #         self.model_class.companies: updated_release.companies,
                        #         self.model_class.country: updated_release.country,
                        #         self.model_class.extra_artists: updated_release.extra_artists,
                        #         self.model_class.formats: updated_release.formats,
                        #         self.model_class.genres: updated_release.genres,
                        #         self.model_class.identifiers: updated_release.identifiers,
                        #         self.model_class.labels: updated_release.labels,
                        #         self.model_class.master_id: updated_release.master_id,
                        #         self.model_class.notes: updated_release.notes,
                        #         self.model_class.release_date: updated_release.release_date,
                        #         self.model_class.styles: updated_release.styles,
                        #         self.model_class.title: updated_release.title,
                        #         self.model_class.tracklist: updated_release.tracklist,
                        #     }
                        # ).where(
                        #     self.model_class.release_id
                        #     == updated_release.release_id
                        # )
                        # q.execute()  # Execute the query.
                        repository.commit()
                        updated_count += 1
                except DatabaseError as e:
                    log.exception("Error in updater_pass_one")
                    raise e

        log.info(
            f"[{proc_name}] processed_count: {self.processed_count}, updated: {updated_count}"
        )
