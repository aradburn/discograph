"""
This module defines the Domain Objects.
"""

import json
from typing import Any, Callable, TypeVar

from pydantic import BaseModel, ConfigDict

__all__ = [
    "InternalDomainObject",
    "_InternalDomainObject",
    "PublicDomainObject",
    "_PublicDomainObject",
]

from discograph import utils
from discograph.library.fields.entity_type import EntityType


def to_camelcase(string: str) -> str:
    """The alias generator for PublicDomainObject."""

    resp = "".join(
        word.capitalize() if index else word
        for index, word in enumerate(string.split("_"))
    )
    return resp


_json_encoders: dict[Any, Callable[[Any], Any]] = {
    EntityType: lambda v: EntityType(v).name,
}


class InternalDomainObject(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        use_enum_values=False,
        validate_assignment=True,
        arbitrary_types_allowed=False,
        from_attributes=True,
        json_encoders=_json_encoders,
    )

    def __repr__(self) -> str:
        return utils.normalize_dict(self.model_dump(exclude={"random"}))


_InternalDomainObject = TypeVar("_InternalDomainObject", bound=InternalDomainObject)


class PublicDomainObject(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        use_enum_values=False,
        validate_assignment=True,
        arbitrary_types_allowed=True,
        from_attributes=True,
        json_encoders=_json_encoders,
        loc_by_alias=True,
        alias_generator=to_camelcase,
    )

    def flat_dict(self, by_alias=True):
        """This method might be useful if the data should be passed
        only with primitives that are allowed by JSON format.
        The regular .model_dump() does not return the ISO datetime format
        but the .model_dump_json() - does.
        This method is just a combination of them both.
        """
        return json.loads(self.model_dump_json(by_alias=by_alias))


_PublicDomainObject = TypeVar("_PublicDomainObject", bound=PublicDomainObject)
