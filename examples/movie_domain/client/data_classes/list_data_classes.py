from .core_list import TypeList
from .data_classes import (
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
    _NODE = Person


class NominationList(TypeList):
    _NODE = Nomination


class BestDirectorList(TypeList):
    _NODE = BestDirector


class BestLeadingActorList(TypeList):
    _NODE = BestLeadingActor


class BestLeadingActressList(TypeList):
    _NODE = BestLeadingActress


class RoleList(TypeList):
    _NODE = Role


class ActorList(TypeList):
    _NODE = Actor


class DirectorList(TypeList):
    _NODE = Director


class RatingList(TypeList):
    _NODE = Rating


class MovieList(TypeList):
    _NODE = Movie
