import datetime
import glob
import gzip
import logging
import os
import re
from xml.dom import minidom
from xml.etree import ElementTree


log = logging.getLogger(__name__)


class Bootstrapper(object):
    # CLASS VARIABLES

    date_regex = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
    date_no_dashes_regex = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
    year_regex = re.compile(r"^\d\d\d\d$")
    is_test = False

    # PUBLIC METHODS

    @staticmethod
    def get_xml_path(tag, test=False):
        data_directory = os.path.abspath(
            os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                "..",
                "data",
            )
        )
        if Bootstrapper.is_test or test:
            glob_pattern = f"discogs_test_{tag}s.xml.gz"
        else:
            glob_pattern = f"discogs_2*_{tag}s.xml.gz"
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
    def parse_release_date(date_string):
        # empty string
        if not date_string:
            return None
        # yyyy-mm-dd
        match = Bootstrapper.date_regex.match(date_string)
        if match:
            year, month, day = match.groups()
            return Bootstrapper.validate_release_date(year, month, day)
        # yyyymmdd
        match = Bootstrapper.date_no_dashes_regex.match(date_string)
        if match:
            year, month, day = match.groups()
            return Bootstrapper.validate_release_date(year, month, day)
        # yyyy
        match = Bootstrapper.year_regex.match(date_string)
        if match:
            year, month, day = match.group(), "1", "1"
            return Bootstrapper.validate_release_date(year, month, day)
        # other: "?", "????", "None", "Unknown"
        return None

    @staticmethod
    def element_to_datetime(element):
        if element is None:
            return None
        date_string = element.text.strip()
        return Bootstrapper.parse_release_date(date_string)

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
    def get_iterator(tag):
        file_path = Bootstrapper.get_xml_path(tag)
        file_pointer = gzip.GzipFile(file_path, "r")
        iterator = Bootstrapper.iterparse(file_pointer, tag)
        iterator = Bootstrapper.clean_elements(iterator)
        return iterator

    @staticmethod
    def iterparse(source, tag):
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
