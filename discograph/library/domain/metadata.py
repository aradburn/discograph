__all__ = [
    "MetadataUncommitted",
    "Metadata",
]

import logging
from datetime import datetime

from discograph.library.domain.base import InternalDomainObject

log = logging.getLogger(__name__)


class _MetadataBase(InternalDomainObject):
    metadata_key: str
    metadata_value: str
    metadata_timestamp: datetime


class MetadataUncommitted(_MetadataBase):
    """This schema is used for creating an instance without an id before it is persisted into the database."""

    pass


class Metadata(_MetadataBase):
    metadata_id: int
    version_id: int = 1
