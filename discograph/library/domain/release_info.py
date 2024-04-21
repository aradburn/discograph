__all__ = [
    "ReleaseInfoUncommitted",
    "ReleaseInfo",
]

from datetime import datetime
from typing import Optional

from discograph.library.domain.base import InternalDomainObject


class _ReleaseInfoBase(InternalDomainObject):
    relation_id: int
    release_id: int
    release_date: Optional[datetime]


class ReleaseInfoUncommitted(_ReleaseInfoBase):
    """This schema is used for creating an instance without an id before it is persisted into the database."""

    pass


class ReleaseInfo(_ReleaseInfoBase):
    """Saved ReleaseInfo representation."""

    pass
