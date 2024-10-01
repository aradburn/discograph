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
    model_config = ConfigDict(alias_generator=lambda field_name: field_name.title())

    label: str
    instruments: List[str]
    description: str
    # mimopage: str


class HornbostelSachs(RootModel):
    root: Dict[str, Instrument]

    def __iter__(self):
        return iter(self.__root__)

    # def __getitem__(self, item):
    #     return self.__root__[item]
