from __future__ import annotations

from typing import List, Optional

from pydantic import Field

from .core import CircularModel, CircularModelApply
from .ids import RoleId


class Person(CircularModel):
    name: Optional[str] = None
    birth_year: Optional[int] = Field(None, alias="birthYear")
    roles: list[RoleId] = []


class PersonApply(CircularModelApply):
    name: str
    birth_year: Optional[int] = None
    roles: List[RoleId] = []


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
