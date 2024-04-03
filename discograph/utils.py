import itertools
import json
import math
import re
import textwrap
from typing import Dict, List

from toolz import count
from unidecode import unidecode

from discograph.library.fields.role_type import RoleType

URLIFY_REGEX = re.compile(r"\s+", re.MULTILINE)
ARG_ROLES_REGEX = re.compile(r"^roles(\[\d*\])?$")
STRIP_PATTERN = re.compile(r"(\(\d+\)|[^(\w\s)]+)")
REMOVE_PUNCTUATION = re.compile(r"[^\w\s]")


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


def normalize_dict(obj: Dict) -> str:
    s = normalize(json.dumps(obj, indent=4, sort_keys=True, default=str))
    return s


def normalize_nested_dict(obj: Dict) -> str:
    s = normalize(json.dumps(obj, indent=4, sort_keys=True))
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
    for c in row.__table__.columns:
        print(f"c.name: {c.name}")
        print(f"    getattr(row, c.name): {getattr(row, c.name)}")
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
