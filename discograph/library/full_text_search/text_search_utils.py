import logging

from discograph import utils

log = logging.getLogger(__name__)


def normalise_search_content(string: str) -> str:
    string = string.lower()
    string = utils.STRIP_PATTERN.sub("", string)
    string = string.strip()
    return string
