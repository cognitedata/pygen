from __future__ import annotations

from typing import ClassVar, List, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, TypeList
from .ids import RoleId

__all__ = ["Person", "PersonApply", "PersonList"]


class Person(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    birth_year: Optional[int] = Field(None, alias="birthYear")
    roles: list[RoleId] = []


class PersonApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    birth_year: Optional[int] = None
    roles: List[RoleId] = []

    def to_node(self) -> dm.NodeApply:
        return dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
                    properties={"name": self.name, "birthYear": self.birth_year},
                )
            ],
        )


class PersonList(TypeList[Person]):
    _NODE = Person
