__all__ = [
    "Release",
]

from datetime import date
from typing import List, Dict, Any, Self

from discograph.library.domain.base import InternalDomainObject


class Release(InternalDomainObject):
    """
    Represents a music release.

    Attributes:
        release_id (int): The unique identifier for the release.
        artists (List[Dict[str, Any]] | None): A list of artists associated with the release.
        companies (List[Dict[str, Any]] | None): A list of companies associated with the release.
        country (str | None): The country where the release was made.
        extra_artists (List[Dict[str, Any]] | None): A list of additional artists associated with the release.
        formats (List[Dict[str, Any]] | None): A list of formats in which the release is available.
        genres (List[str] | None): A list of genres associated with the release.
        identifiers (List[Dict[str, Any]] | None): A list of identifiers for the release.
        labels (List[Dict[str, Any]] | None): A list of labels associated with the release.
        master_id (int | None): The master ID of the release.
        notes (str | None): Additional notes about the release.
        release_date (date | None): The release date.
        styles (List[str] | None): A list of styles associated with the release.
        title (str): The title of the release.
        tracklist (List[Dict[str, Any]] | None): The tracklist of the release.
    """

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

    def to_domain(self) -> Self:
        """
        Converts the release to its domain representation.

        Returns:
            Self: The domain representation of the release.
        """
        # Domain and Database entities are the same
        return self

    def to_db(self) -> Self:
        """
        Converts the release to its database representation.

        Returns:
            Self: The database representation of the release.
        """
        # Domain and Database entities are the same
        return self
