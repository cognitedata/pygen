from __future__ import annotations

from typing import ClassVar, List, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from .core import CircularModel, CircularModelApply
from .ids import RoleId


class Person(CircularModel):
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


# class Nomination(CircularModelCore):
#     name: str
#     year: int
#
#
# class BestDirector(Nomination):
#     ...
#
#
# class BestLeadingActor(Nomination):
#     ...
#
#
# class BestLeadingActress(Nomination):
#     ...
#
#
# class Role(CircularModelCore):
#     movies: Optional[List["Movie"]] = None
#     won_oscar: Optional[bool] = None
#     nomination: Optional[List[Nomination]] = None
#     person: Optional["Person"] = None
#
#     @validator("won_oscar", pre=True)
#     def parse_string(cls, value):
#         return value.casefold() == "true" if isinstance(value, str) else value
#
#
# class Actor(Role):
#     ...
#
#
# class Director(Role):
#     ...
#
#
# class Rating(CircularModelCore):
#     score: TimeSeries
#     votes: TimeSeries
#
#
# class Movie(CircularModelCore):
#     title: str
#     actors: Optional[List[Optional[Actor]]] = None
#     directors: Optional[List[Optional[Director]]] = None
#     release_year: Optional[int] = None
#     rating: Optional[Rating] = None
#     run_time_minutes: Optional[float] = None
#     meta: Optional[dict]
#
#
# Actor.update_forward_refs()
# Director.update_forward_refs()
# Person.update_forward_refs()
