__all__ = [
    "Release",
]

from datetime import date
from typing import Optional, List, Dict, Any

from discograph.library.domain.base import InternalDomainObject


class Release(InternalDomainObject):
    release_id: int
    artists: Optional[List[Dict[str, Any]]]
    companies: Optional[List[Dict[str, Any]]]
    country: Optional[str]
    extra_artists: Optional[List[Dict[str, Any]]]
    formats: Optional[List[Dict[str, Any]]]
    genres: Optional[List[str]]
    identifiers: Optional[List[Dict[str, Any]]]
    labels: Optional[List[Dict[str, Any]]]
    master_id: Optional[int]
    notes: Optional[str]
    release_date: Optional[date]
    styles: Optional[List[str]]
    title: str
    tracklist: Optional[List[Dict[str, Any]]]
    random: float
