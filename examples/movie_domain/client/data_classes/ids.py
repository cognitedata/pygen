from __future__ import annotations

from typing import ClassVar, TypeVar

from cognite.client import data_modeling as dm
from pydantic import BaseModel, constr


class Identifier(BaseModel):
    _instance_type: ClassVar[str] = "node"
    space: constr(min_length=1, max_length=255)
    external_id: constr(min_length=1, max_length=255)

    @classmethod
    def from_direct_relation(cls, relation: dm.DirectRelationReference) -> T_Identifier:
        return cls(space=relation.space, external_id=relation.external_id)

    def __str__(self):
        return f"{self.space}/{self.external_id}"


T_Identifier = TypeVar("T_Identifier", bound=Identifier)


class RoleId(Identifier):
    ...
