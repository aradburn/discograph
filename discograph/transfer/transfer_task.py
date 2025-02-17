import logging

import luigi
from luigi.contrib.simulate import RunAnywayTarget

from discograph.transfer.transfer_manager import TransferManager

log = logging.getLogger(__name__)


class TransferTask(luigi.Task):

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
        TransferManager.transfer_all()
        self.output().done()
