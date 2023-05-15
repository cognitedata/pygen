from movie_domain.sdk.core import TypeList
from movie_domain.sdk.data_classes import (
    Actor,
    BestDirector,
    BestLeadingActor,
    BestLeadingActress,
    Director,
    Movie,
    Nomination,
    Person,
    Rating,
    Role,
)


class PersonList(TypeList):
    _RESOURCE = Person


class NominationList(TypeList):
    _RESOURCE = Nomination


class BestDirectorList(TypeList):
    _RESOURCE = BestDirector


class BestLeadingActorList(TypeList):
    _RESOURCE = BestLeadingActor


class BestLeadingActressList(TypeList):
    _RESOURCE = BestLeadingActress


class RoleList(TypeList):
    _RESOURCE = Role


class ActorList(TypeList):
    _RESOURCE = Actor


class DirectorList(TypeList):
    _RESOURCE = Director


class RatingList(TypeList):
    _RESOURCE = Rating


class MovieList(TypeList):
    _RESOURCE = Movie
