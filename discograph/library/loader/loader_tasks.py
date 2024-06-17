import datetime
import logging
import os
from urllib.parse import urlparse

import luigi

from discograph.config import (
    ROOT_DIR,
    DISCOGS_ARTISTS_TYPE,
    DISCOGS_RELEASES_TYPE,
    DISCOGS_LABELS_TYPE,
    DISCOGS_MASTERS_TYPE,
)
from discograph.utils import (
    get_discogs_dump_dates,
    download_file,
    get_discogs_url,
)

log = logging.getLogger(__name__)


class LoaderTask(luigi.WrapperTask):
    start_date: datetime.date = luigi.DateParameter()
    end_date: datetime.date = luigi.DateParameter()

    def requires(self):
        # tasks = []
        dates = get_discogs_dump_dates(self.start_date, self.end_date)
        for date in dates:
            yield LoaderTaskForDate(dump_date=date)
            # tasks.append(LoaderTaskForDate(dump_date=date))
        # return tasks


class LoaderTaskForDate(luigi.WrapperTask):
    dump_date: datetime.date = luigi.DateParameter()

    @property
    def priority(self):
        diff = (
            datetime.datetime.now()
            - datetime.datetime(
                year=self.dump_date.year,
                month=self.dump_date.month,
                day=self.dump_date.day,
            )
        ).total_seconds()
        log.debug(f"priority: {diff}")
        return diff

    def requires(self):
        tasks = [
            DiscogsDownloaderTask(
                dump_date=self.dump_date, dump_type=DISCOGS_ARTISTS_TYPE
            ),
            DiscogsDownloaderTask(
                dump_date=self.dump_date, dump_type=DISCOGS_RELEASES_TYPE
            ),
            DiscogsDownloaderTask(
                dump_date=self.dump_date, dump_type=DISCOGS_LABELS_TYPE
            ),
            DiscogsDownloaderTask(
                dump_date=self.dump_date, dump_type=DISCOGS_MASTERS_TYPE
            ),
        ]
        return tasks


class DiscogsDownloaderTask(luigi.Task):
    dump_date: datetime.date = luigi.DateParameter()
    dump_type: str = luigi.Parameter()

    @property
    def url(self) -> str:
        return get_discogs_url(self.dump_date, self.dump_type)

    def requires(self):
        return None

    def output(self) -> luigi.LocalTarget:
        output_url = urlparse(self.url)
        filename = output_url.path.rsplit("/", 1)[-1]
        filepath = os.path.join(ROOT_DIR, "discograph", "data", filename)
        log.debug(f"DiscogsArtistsDownloaderTask output: {filepath}")

        return luigi.LocalTarget(filepath)

    def run(self):
        log.debug(f"Running task: {self.task_id} for date: {self.dump_date}")
        log.debug(f"download_file({self.url}, {self.output().path})")
        # with self.output().open("wb") as output_file:
        #     download_file(self.url, output_file)
        with self.output().temporary_path() as temporary_binary_file_path:
            with open(temporary_binary_file_path, "wb") as output_file:
                download_file(self.url, output_file)


# class EntityPassOneTask(luigi.Task):
#     dump_date: datetime.date = luigi.DateParameter()
#
#     def requires(self):
#         return OtherTask(self.date), DailyReport(self.date - datetime.timedelta(1))
#
#     def output(self):
#         return LoaderTarget(self, self.dump_date)
#
#     def run(self):
#         with self.output().open("w") as outfile:
#             outfile.write("Hello Luigi!")
#         self.output().done()
