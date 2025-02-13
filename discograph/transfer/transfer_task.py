import logging

import luigi
from luigi.contrib.simulate import RunAnywayTarget

from discograph.offline.offline_database_manager import OfflineDatabaseManager
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
from discograph.transfer.transfer_manager import TransferManager

log = logging.getLogger(__name__)


class TransferTask(luigi.Task):
    offline_database_manager: OfflineDatabaseManager = luigi.Parameter()
    runtime_database_manager: RuntimeDatabaseManager = luigi.Parameter()

    def output(self):
        # Always run this task
        return RunAnywayTarget(self)

    def requires(self):
        return None

    @property
    def priority(self):
        return -1000000000

    def run(self):
        log.debug(f"Running transfer task: {self.task_id}")
        TransferManager.transfer_all(self.runtime_database_manager)
        self.output().done()
