import datetime
import logging
import os
from urllib.parse import urlparse

import luigi
from luigi.contrib.simulate import RunAnywayTarget
from luigi.mock import MockTarget

from discograph.config import (
    DISCOGS_ARTISTS_TYPE,
    DISCOGS_RELEASES_TYPE,
    DISCOGS_LABELS_TYPE,
    DISCOGS_MASTERS_TYPE,
    ROOT_DIR,
    PostgresTestConfiguration,
    DATA_DIR,
)
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.loader.loader_target import LoaderTarget
from discograph.library.relation_grapher import RelationGrapher
from discograph.utils import get_discogs_dump_dates, get_discogs_url
from tests.integration.library.database.repository_test_case import RepositoryTestCase

log = logging.getLogger("tests.integration." + __name__)


class TestLoaderTask(luigi.WrapperTask):
    start_date: datetime.date = luigi.DateParameter()
    end_date: datetime.date = luigi.DateParameter()

    def requires(self):
        yield TestLoaderSetupTask(start_date=self.start_date, end_date=self.end_date)
        dates = get_discogs_dump_dates(self.start_date, self.end_date)
        for date in dates:
            yield TestDiscogsDownloaderTaskForDate(dump_date=date)
            yield TestLoaderTaskForDate(dump_date=date)


class TestDiscogsDownloaderTaskForDate(luigi.WrapperTask):
    dump_date: datetime.date = luigi.DateParameter()

    @property
    def priority(self):
        diff = int(
            (
                datetime.datetime.now()
                - datetime.datetime(
                    year=self.dump_date.year,
                    month=self.dump_date.month,
                    day=self.dump_date.day,
                )
            ).total_seconds()
        )
        log.debug(f"TestDiscogsDownloaderTaskForDate priority: {diff}")
        return diff

    def requires(self):
        yield TestDiscogsDownloaderTask(
            dump_date=self.dump_date, dump_type=DISCOGS_ARTISTS_TYPE
        )
        yield TestDiscogsDownloaderTask(
            dump_date=self.dump_date, dump_type=DISCOGS_RELEASES_TYPE
        )
        yield TestDiscogsDownloaderTask(
            dump_date=self.dump_date, dump_type=DISCOGS_LABELS_TYPE
        )
        yield TestDiscogsDownloaderTask(
            dump_date=self.dump_date, dump_type=DISCOGS_MASTERS_TYPE
        )


class TestLoaderTaskForDate(luigi.WrapperTask):
    dump_date: datetime.date = luigi.DateParameter()

    @property
    def priority(self):
        diff = int(
            (
                datetime.datetime.now()
                - datetime.datetime(
                    year=self.dump_date.year,
                    month=self.dump_date.month,
                    day=self.dump_date.day,
                )
            ).total_seconds()
        )
        log.debug(f"TestLoaderTaskForDate priority: {diff}")
        return diff

    def requires(self):
        yield TestDiscogsDownloaderTaskForDate(dump_date=self.dump_date)
        stages = DatabaseHelper.db_helper.get_load_table_stages(
            DATA_DIR, self.dump_date.strftime("%Y%m%d"), is_bulk_inserts=False
        )
        for stage in range(0, len(stages)):
            yield TestLoaderTaskForDateAndStage(dump_date=self.dump_date, stage=stage)


class TestLoaderTaskForDateAndStage(luigi.Task):
    dump_date: datetime.date = luigi.DateParameter()
    stage: int = luigi.IntParameter()

    @property
    def priority(self):
        diff = int(
            (
                datetime.datetime.now()
                - datetime.datetime(
                    year=self.dump_date.year,
                    month=self.dump_date.month,
                    day=self.dump_date.day,
                )
            ).total_seconds()
        ) + (100 - self.stage)
        log.debug(
            f"TestLoaderTaskForDateAndStage date: {self.dump_date} stage: {self.stage} priority: {diff}"
        )
        return diff

    def requires(self):
        if self.stage > 0:
            yield TestLoaderTaskForDateAndStage(
                dump_date=self.dump_date, stage=self.stage - 1
            )
        else:
            pass

    def output(self):
        # Store the outcome of the task as a record in the database
        return LoaderTarget(self, self.dump_date)

    def run(self):
        log.debug(
            f"Run TestLoaderTaskForDateAndStage tasks for stage: {self.stage} date: {self.dump_date}"
        )
        stages = DatabaseHelper.db_helper.get_load_table_stages(
            DATA_DIR, self.dump_date.strftime("%Y%m%d"), is_bulk_inserts=False
        )
        log.debug(f"Run stage: {self.stage} of {len(stages)}")
        # dummy run stages[self.stage]()
        # Mark task done in the database
        self.output().done()


class TestDiscogsDownloaderTask(luigi.Task):
    dump_date: datetime.date = luigi.DateParameter()
    dump_type: str = luigi.Parameter()

    @property
    def url(self) -> str:
        return get_discogs_url(self.dump_date, self.dump_type)

    def output(self) -> MockTarget:
        output_url = urlparse(self.url)
        filename = output_url.path.rsplit("/", 1)[-1]
        filepath = os.path.join(ROOT_DIR, "tests", "data", filename)
        # log.debug(f"TestDiscogsDownloaderTask output: {filepath}")
        return MockTarget(filepath)

    def run(self):
        log.debug(f"Running task: {self.task_id} for date: {self.dump_date}")
        log.debug(f"download_file({self.url}, {self.output().path})")
        with self.output().open("wb") as output_file:
            output_file.write(self.url)


class TestLoaderSetupTask(luigi.Task):
    start_date: datetime.date = luigi.DateParameter()
    end_date: datetime.date = luigi.DateParameter()

    def output(self):
        # Always run this task
        return RunAnywayTarget(self)

    def run(self):
        log.debug(f"Running loader setup task: {self.task_id}")
        logger1 = logging.getLogger("luigi")
        logger2 = logging.getLogger("luigi-interface")
        log.debug(f"logger1: {logger1}")
        log.debug(f"logger2: {logger2}")
        logging.getLogger("luigi").handlers = logging.getLogger("discograph").handlers
        logging.getLogger("luigi").propagate = False
        logging.getLogger("luigi").setLevel(logging.WARNING)
        logging.getLogger("luigi-interface").handlers = logging.getLogger(
            "discograph"
        ).handlers
        logging.getLogger("luigi-interface").propagate = False
        logging.getLogger("luigi-interface").setLevel(logging.WARNING)
        log.debug(f"logger1: {logger1}")
        log.debug(f"logger2: {logger2}")
        self.output().done()

        yield TestLoaderTask(start_date=self.start_date, end_date=self.end_date)


class TestTaskRunner(RepositoryTestCase):

    @classmethod
    def setUpClass(cls):
        RepositoryTestCase._config = PostgresTestConfiguration()
        RepositoryTestCase.relation_grapher = RelationGrapher
        super().setUpClass()

    def test_run_tasks(self):
        start_date = datetime.date(2023, 10, 1)
        end_date = datetime.datetime.now()
        tasks = [TestLoaderSetupTask(start_date=start_date, end_date=end_date)]

        luigi_run_result = luigi.build(
            tasks,
            detailed_summary=True,
            local_scheduler=True,
            log_level="WARNING",
        )
        print(luigi_run_result.summary_text)
