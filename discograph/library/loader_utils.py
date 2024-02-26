import datetime
import glob
import gzip
import logging
import os
import re
from typing import Optional
from xml.dom import minidom
from xml.etree import ElementTree


log = logging.getLogger(__name__)


class LoaderUtils:
    # CLASS CONSTANTS

    DATE_REGEX = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
    DATE_NO_DASHES_REGEX = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
    YEAR_REGEX = re.compile(r"^\d\d\d\d$")

    # PUBLIC STATIC METHODS

    @staticmethod
    def get_xml_path(tag: str, date: str = ""):
        # Date in the format yyyymmdd, use test_yyyymmdd for test data
        data_directory = os.path.abspath(
            os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                "..",
                "data",
            )
        )
        glob_pattern = f"discogs_{date}_{tag}s.xml.gz"
        log.debug(f"data_directory: {data_directory}")
        log.debug(f"glob_pattern: {glob_pattern}")
        # with contextmanagers.TemporaryDirectoryChange(data_directory):
        files = sorted(glob.glob(glob_pattern, root_dir=data_directory))
        log.debug(f"files: {files}")
        full_path_files = os.path.join(data_directory, files[-1])
        log.debug(f"full_path_files: {full_path_files}")
        return full_path_files

    @staticmethod
    def clean_elements(elements):
        for element in elements:
            image_tags = element.findall("images")
            if image_tags:
                element.remove(*image_tags)
            # url_tags = element.findall('urls')
            # if url_tags:
            #    element.remove(*url_tags)
            yield element

    @staticmethod
    def parse_release_date(date_string: Optional[str]):
        # empty string
        if not date_string:
            return None
        # yyyy-mm-dd
        match = LoaderUtils.DATE_REGEX.match(date_string)
        if match:
            year, month, day = match.groups()
            return LoaderUtils.validate_release_date(year, month, day)
        # yyyymmdd
        match = LoaderUtils.DATE_NO_DASHES_REGEX.match(date_string)
        if match:
            year, month, day = match.groups()
            return LoaderUtils.validate_release_date(year, month, day)
        # yyyy
        match = LoaderUtils.YEAR_REGEX.match(date_string)
        if match:
            year, month, day = match.group(), "1", "1"
            return LoaderUtils.validate_release_date(year, month, day)
        # other: "?", "????", "None", "Unknown"
        return None

    @staticmethod
    def element_to_datetime(element):
        if element is None:
            return None
        date_string = element.text.strip()
        return LoaderUtils.parse_release_date(date_string)

    @staticmethod
    def element_to_integer(element):
        if element is not None:
            return int(element.text)
        return None

    @staticmethod
    def element_to_string(element):
        if element is not None:
            return element.text or None
        return None

    @staticmethod
    def element_to_strings(element):
        if element is not None and len(element):
            return [_.text for _ in element]
        return None

    @staticmethod
    def get_iterator(tag: str, date: str):
        file_path = LoaderUtils.get_xml_path(tag, date)
        file_pointer = gzip.GzipFile(file_path, "r")
        iterator = LoaderUtils.iterparse(file_pointer, tag)
        iterator = LoaderUtils.clean_elements(iterator)
        return iterator

    @staticmethod
    def iterparse(source, tag: str):
        context = ElementTree.iterparse(source, events=("start", "end"))
        context = iter(context)
        _, root = next(context)
        depth = 0
        for event, element in context:
            if element.tag == tag:
                if event == "start":
                    depth += 1
                else:
                    depth -= 1
                    if depth == 0:
                        yield element
                        root.clear()

    @staticmethod
    def prettify(element):
        string = ElementTree.tostring(element, "utf-8")
        reparsed = minidom.parseString(string)
        return reparsed.toprettyxml(indent="    ")

    @staticmethod
    def validate_release_date(year: str, month: str, day: str):
        try:
            year = int(year)
            if month.isdigit():
                month = int(month)
            if month < 1:
                month = 1
            if day.isdigit():
                day = int(day)
            if day < 1:
                day = 1
            if 12 < month:
                day, month = month, day
            date = datetime.datetime(year, month, 1, 0, 0)
            day_offset = day - 1
            date = date + datetime.timedelta(days=day_offset)
        except ValueError:
            log.exception("BAD DATE:", year, month, day)
            date = None
        return date
