from typing import List, Optional

from pydantic import validator

from .core import CircularModel, TimeSeries


class Person(CircularModel):
    name: str
    birth_year: Optional[int] = None
    roles: Optional[List["Role"]] = None


class Nomination(CircularModel):
    name: str
    year: int


class BestDirector(Nomination):
    ...


class BestLeadingActor(Nomination):
    ...


class BestLeadingActress(Nomination):
    ...


class Role(CircularModel):
    movies: Optional[List["Movie"]] = None
    won_oscar: Optional[bool] = None
    nomination: Optional[List[Nomination]] = None
    person: Optional["Person"] = None

    @validator("won_oscar", pre=True)
    def parse_string(cls, value):
        return value.casefold() == "true" if isinstance(value, str) else value


class Actor(Role):
    ...


class Director(Role):
    ...


class Rating(CircularModel):
    score: TimeSeries
    votes: TimeSeries


class Movie(CircularModel):
    title: str
    actors: Optional[List[Optional[Actor]]] = None
    directors: Optional[List[Optional[Director]]] = None
    release_year: Optional[int] = None
    rating: Optional[Rating] = None
    run_time_minutes: Optional[float] = None
    meta: Optional[dict]


Actor.update_forward_refs()
Director.update_forward_refs()
Person.update_forward_refs()
