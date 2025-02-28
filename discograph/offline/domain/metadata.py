__all__ = [
    "MetadataUncommitted",
    "Metadata",
]

import logging
from datetime import datetime
from typing import Self

from discograph.library.domain.base import InternalDomainObject

log = logging.getLogger(__name__)


class _MetadataBase(InternalDomainObject):
    """
    Base class for metadata entities.

    Attributes:
        metadata_key (str): The key of the metadata.
        metadata_value (str): The value of the metadata.
        metadata_timestamp (datetime): The timestamp when the metadata was created or updated.
    """

    metadata_key: str
    metadata_value: str
    metadata_timestamp: datetime


class MetadataUncommitted(_MetadataBase):
    """
    Schema used for creating an instance without an ID before it is persisted into the database.
    """

    pass


class Metadata(_MetadataBase):
    """
    Metadata entity that includes an ID and version information.

    Attributes:
        metadata_id (int): The unique identifier for the metadata.
        version_id (int): The version of the metadata, default is 1.
    """

    metadata_id: int
    version_id: int = 1

    def to_domain(self) -> Self:
        """
        Converts the metadata to its domain representation.

        Returns:
            Self: The domain representation of the metadata.
        """
        return self

    def to_db(self) -> Self:
        """
        Converts the metadata to its database representation.

        Returns:
            Self: The database representation of the metadata.
        """
        return self
