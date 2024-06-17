import datetime
import logging
import luigi

from discograph.exceptions import NotFoundError
from discograph.library.database.metadata_repository import MetadataRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.metadata import MetadataUncommitted

log = logging.getLogger(__name__)


class LoaderTarget(luigi.Target):

    def __init__(self, task_obj: luigi.Task, date: datetime.date):
        self.task_id = task_obj.task_id
        self.date = date

    def __str__(self):
        return self.task_id

    def get_key(self) -> str:
        return f"task-{self.task_id}-{self.date}"

    def exists(self):
        """
        Checks if the target exists
        """
        key = self.get_key()
        with transaction():
            repository = MetadataRepository()
            try:
                log.debug(f"Checking task key: {key}")

                created_metadata = repository.get_by_key(key)
                if created_metadata is not None:
                    metadata_exists = True
            except NotFoundError:
                metadata_exists = False
        log.debug(f"key exists: {metadata_exists}")
        return metadata_exists

    def done(self):
        """
        Creates a database record to mark the task as done
        """
        key = self.get_key()
        metadata = MetadataUncommitted(
            metadata_key=key,
            metadata_value="done",
            metadata_timestamp=datetime.datetime.now(),
        )

        # WHEN
        with transaction():
            repository = MetadataRepository()
            repository.create(metadata)
            log.debug(f"Created task done key: {key}")

        log.info(f"Marking {key} as done")
