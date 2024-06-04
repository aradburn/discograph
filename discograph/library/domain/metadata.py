__all__ = [
    "Metadata",
]

import logging
from datetime import datetime

from discograph.library.domain.base import InternalDomainObject

log = logging.getLogger(__name__)


class Metadata(InternalDomainObject):
    load_timestamp: datetime
    load_stage: str
    load_block: int
