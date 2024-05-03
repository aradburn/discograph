import enum
import itertools
import json
import logging
import math
import re
import textwrap
import time
from collections.abc import Mapping
from datetime import datetime, date
from functools import wraps
from random import random
from typing import Dict, List, Any

from toolz import count
from unidecode import unidecode

from discograph.library.database.database_helper import Base
from discograph.library.fields.role_type import RoleType

log = logging.getLogger(__name__)

URLIFY_REGEX = re.compile(r"\s+", re.MULTILINE)
ARG_ROLES_REGEX = re.compile(r"^roles(\[\d*\])?$")
STRIP_PATTERN = re.compile(r"(\(\d+\)|[^(\w\s)]+)")
REMOVE_PUNCTUATION = re.compile(r"[^\w\s]")
WORD_PATTERN = re.compile(r"\s+")


class SkipFilter:
    def __init__(self, types=None, keys=None, allow_empty=False):
        self.types = tuple(types or [])
        self.keys = set(keys or [])
        self.allow_empty = allow_empty  # if True include empty filtered structures

    def filter(self, data):
        if isinstance(data, Mapping):
            result = {}  # dict-like, use dict as a base
            for k, v in data.items():
                if k in self.keys or isinstance(v, self.types):  # skip key/type
                    continue
                try:
                    result[k] = self.filter(v)
                except ValueError:
                    pass
            if result or self.allow_empty:
                return result
        # elif isinstance(data, Sequence):
        #     result = []  # a sequence, use list as a base
        #     for v in data:
        #         if isinstance(v, self.types):  # skip type
        #             continue
        #         try:
        #             result.append(self.filter(v))
        #         except ValueError:
        #             pass
        #     if result or self.allow_empty:
        #         return result
        else:  # we don't know how to traverse this structure...
            return data  # return it as-is, hope for the best...
        raise ValueError


def parse_request_args(args):
    year = None
    roles = set()
    for key in args:
        if key == "year":
            year = args[key]
            try:
                if "-" in year:
                    start, _, stop = year.partition("-")
                    year = tuple(sorted((int(start), int(stop))))
                else:
                    year = int(year)
            finally:
                pass
        elif ARG_ROLES_REGEX.match(key):
            value = args.getlist(key)
            for role in value:
                if role in RoleType.role_definitions:
                    roles.add(role)
    roles = list(sorted(roles))
    return roles, year


def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


# def iter_in_slices(iterator, size=None):
#     while True:
#         slice_iter = itertools.islice(iterator, size)
#         # If no first object this is how StopIteration is triggered
#         try:
#             peek = next(slice_iter)
#         except StopIteration:
#             return
#         # Put the first object back and return slice
#         yield itertools.chain([peek], slice_iter)


def split_tuple(num_chunks: int, seq):
    num_items = count(seq)
    # print(f"num_items: {num_items}")
    num_chunks = min(num_items, num_chunks)
    num_chunks = max(1, num_chunks)
    # print(f"num_chunks: {num_chunks}")
    return batched(seq, math.ceil(num_items / num_chunks))
    # return list(iter_in_slices(seq, math.ceil(num_items / num_chunks)))
    # return partition_all(math.ceil(num_items / num_chunks), seq)


def normalize(argument: str, indent: int | str | None = None) -> str:
    """
    Normalizes string.

    ..  container:: example

        >>> string = r'''
        ...     foo
        ...         bar
        ... '''
        >>> print(string)
        <BLANKLINE>
            foo
                bar
        <BLANKLINE>

        >>> print(utils.normalize(string))
        foo
            bar

        >>> print(utils.normalize(string, indent=4))
            foo
                bar

        >>> print(utils.normalize(string, indent='* '))
        * foo
        *     bar

    """
    string = argument.replace("\t", "    ")
    lines = string.split("\n")
    while lines and (not lines[0] or lines[0].isspace()):
        lines.pop(0)
    while lines and (not lines[-1] or lines[-1].isspace()):
        lines.pop()
    for i, line in enumerate(lines):
        lines[i] = line.rstrip()
    string = "\n".join(lines)
    string = textwrap.dedent(string)
    if indent:
        if isinstance(indent, str):
            indent_string = indent
        else:
            assert isinstance(indent, int)
            indent_string = abs(int(indent)) * " "
        lines = string.split("\n")
        for i, line in enumerate(lines):
            if line:
                lines[i] = f"{indent_string}{line}"
        string = "\n".join(lines)
    if not string.endswith("\n"):
        string += "\n"
    return string


# def normalize_dict(obj: Dict) -> str:
#     s = normalize(json.dumps(obj, indent=4, sort_keys=True, default=str))
#     return s


def normalize_dict(obj: Any, skip_keys=None) -> str:
    if skip_keys is None:
        skip_keys = ["random"]
    preprocessor = SkipFilter(keys=skip_keys)

    def list_public_attributes(input_var):
        return {
            k: list_public_attributes(preprocessor.filter(v))
            if isinstance(v, Mapping)
            else v
            for k, v in input_var.items()
            if not (k.startswith("_") or callable(v))
        }

    def default(o):
        def as_dict(self):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}

        if isinstance(o, Base):
            return list_public_attributes(preprocessor.filter(as_dict(o)))
        elif isinstance(o, enum.Enum):
            return str(o)
        elif isinstance(o, date):
            return str(o)
        elif isinstance(o, datetime):
            return str(o)
        else:
            return f"<<non-serializable: {type(o).__qualname__}>>"

    s = normalize(
        json.dumps(
            obj,
            indent=4,
            sort_keys=True,
            default=default,
        )
    )
    return s


def normalize_dict_list(list_obj: List[Dict]) -> str:
    return (
        "[\n"
        + ",\n".join(
            textwrap.indent(
                strip_trailing_newline(
                    normalize(json.dumps(_, indent=4, sort_keys=True, default=str))
                ),
                "    ",
            )
            for _ in list_obj
        )
        + "\n]\n"
    )


def normalize_str_list(list_obj: List[str]) -> str:
    return "[\n" + ",\n".join(textwrap.indent(_, "    ") for _ in list_obj) + "\n]\n"


def strip_input(input_str: str) -> str:
    return textwrap.dedent(input_str).replace("\n", "", 1)


def strip_trailing_newline(input_str: str) -> str:
    return input_str.removesuffix("\n")


def row2dict(row):
    # for c in row.__table__.columns:
    #     print(f"c.name: {c.name}")
    #     print(f"    getattr(row, c.name): {getattr(row, c.name)}")
    return {c.name: getattr(row, c.name) for c in row.__table__.columns}


def to_ascii(string: str) -> str:
    # Transliterate the unicode string into a plain ASCII string
    string = unidecode(string, "preserve")
    return string


def normalise_search_content(string: str) -> str:
    string = string.lower()
    string = to_ascii(string)
    string = STRIP_PATTERN.sub("", string)
    string = REMOVE_PUNCTUATION.sub("", string)
    return string


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        log.debug(f"### TIMER ### Function {func.__name__}: {total_time:.1f} seconds")
        return result

    return timeit_wrapper


def sleep_with_backoff(multiplier: int) -> None:
    time_in_secs = multiplier * (1.0 + 4.0 * random())
    log.debug(f"sleeping for {time_in_secs} secs")
    time.sleep(time_in_secs)
