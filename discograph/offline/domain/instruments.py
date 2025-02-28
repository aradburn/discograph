__all__ = [
    "Instrument",
    "HornbostelSachs",
]

import logging
from typing import List, Dict

from pydantic import ConfigDict, RootModel

from discograph.library.domain.base import InternalDomainObject

log = logging.getLogger(__name__)


class Instrument(InternalDomainObject):
    """
    Represents a musical instrument.

    Attributes:
        label (str): The label of the instrument.
        instruments (List[str]): A list of instrument names.
        description (str): A description of the instrument.
    """

    model_config = ConfigDict(alias_generator=lambda field_name: field_name.title())

    label: str
    instruments: List[str]
    description: str
    # mimopage: str


class HornbostelSachs(RootModel):
    """
    Represents the Hornbostel-Sachs classification system for musical instruments.

    Attributes:
        root (Dict[str, Instrument]): A dictionary mapping instrument categories to Instrument objects.
    """

    root: Dict[str, Instrument]

    def __iter__(self):
        """
        Returns an iterator over the root dictionary.

        Returns:
            Iterator: An iterator over the root dictionary.
        """
        return iter(self.__root__)

    # def __getitem__(self, item):
    #     return self.__root__[item]
