__all__ = [
    "Release",
]

from datetime import date
from typing import List, Dict, Any

from discograph.library.domain.base import InternalDomainObject


class Release(InternalDomainObject):
    release_id: int
    artists: List[Dict[str, Any]] | None = None
    companies: List[Dict[str, Any]] | None = None
    country: str | None = None
    extra_artists: List[Dict[str, Any]] | None = None
    formats: List[Dict[str, Any]] | None = None
    genres: List[str] | None = None
    identifiers: List[Dict[str, Any]] | None = None
    labels: List[Dict[str, Any]] | None = None
    master_id: int | None = None
    notes: str | None = None
    release_date: date | None = None
    styles: List[str] | None = None
    title: str
    tracklist: List[Dict[str, Any]] | None = None
    random: float
