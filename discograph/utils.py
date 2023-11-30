import multiprocessing
import re

from discograph.config import ThreadingModel
from discograph.database import threading_model


urlify_pattern = re.compile(r"\s+", re.MULTILINE)
args_roles_pattern = re.compile(r'^roles(\[\d*\])?$')


def get_concurrency_count():
    if threading_model == ThreadingModel.PROCESS:
        return multiprocessing.cpu_count() * 2
    elif threading_model == ThreadingModel.THREAD:
        return 1
    else:
        NotImplementedError("THREADING_MODEL not configured")


def parse_request_args(args):
    from discograph.library import CreditRole
    year = None
    roles = set()
    for key in args:
        if key == 'year':
            year = args[key]
            try:
                if '-' in year:
                    start, _, stop = year.partition('-')
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
