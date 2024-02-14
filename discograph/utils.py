import math
import re
import textwrap

from toolz import partition_all

urlify_pattern = re.compile(r"\s+", re.MULTILINE)
args_roles_pattern = re.compile(r"^roles(\[\d*\])?$")


def parse_request_args(args):
    from discograph.library import CreditRole

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
        elif args_roles_pattern.match(key):
            value = args.getlist(key)
            for role in value:
                if role in CreditRole.all_credit_roles:
                    roles.add(role)
    roles = list(sorted(roles))
    return roles, year


def split_tuple(num_chunks: int, seq):
    num_items = len(seq)
    num_chunks = min(num_items, num_chunks)
    num_chunks = max(1, num_chunks)
    return partition_all(math.ceil(num_items / num_chunks), seq)


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
    return string
