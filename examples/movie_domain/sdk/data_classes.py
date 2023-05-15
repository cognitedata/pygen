from typing import List, Optional

from movie_domain.sdk.core import DomainModel, TimeSeries


class Person(DomainModel):
    name: str
    birth_year: Optional[int] = None
    roles: List[Optional["Role"]] = []


class Nomination(DomainModel):
    name: str
    year: int


class BestDirector(Nomination):
    ...


class BestLeadingActor(Nomination):
    ...


class BestLeadingActress(Nomination):
    ...


class Role(DomainModel):
    movies: List["Movie"] = []
    won_oscar: Optional[bool] = None
    nomination: List[Optional[Nomination]] = []
    person: "Person"


class Actor(Role):
    ...


class Director(Role):
    ...


class Rating(DomainModel):
    score: TimeSeries
    votes: TimeSeries


class Movie(DomainModel):
    title: str
    actors: List[Optional[Actor]] = []
    directors: List[Optional[Director]] = []
    release_year: Optional[int] = None
    rating: Optional[Rating] = None
    run_time_minutes: Optional[float] = None
    meta: Optional[dict]
