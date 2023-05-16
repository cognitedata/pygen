from typing import List, Optional

from pydantic import validator

from .core import CircularModel, TimeSeries


class Person(CircularModel):
    name: str
    birth_year: Optional[int] = None
    roles: List[Optional["Role"]] = []


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
    movies: List["Movie"] = []
    won_oscar: Optional[bool] = None
    nomination: List[Optional[Nomination]] = []
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
    actors: List[Optional[Actor]] = []
    directors: List[Optional[Director]] = []
    release_year: Optional[int] = None
    rating: Optional[Rating] = None
    run_time_minutes: Optional[float] = None
    meta: Optional[dict]
