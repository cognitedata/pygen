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

# class NominationList(TypeList):
#     _NODE = Nomination
#
#
# class BestDirectorList(TypeList):
#     _NODE = BestDirector
#
#
# class BestLeadingActorList(TypeList):
#     _NODE = BestLeadingActor
#
#
# class BestLeadingActressList(TypeList):
#     _NODE = BestLeadingActress
#
#
# class RoleList(TypeList):
#     _NODE = Role
#
#
# class ActorList(TypeList):
#     _NODE = Actor
#
#
# class DirectorList(TypeList):
#     _NODE = Director
#
#
# class RatingList(TypeList):
#     _NODE = Rating
#
#
# class MovieList(TypeList):
#     _NODE = Movie

#
# class DirectorAPI(TypeAPI):
#     ...
#
#
# class MovieAPI(TypeAPI):
#     ...
#
#
# class ActorsAPI(TypeAPI):
#     ...
#
#
# class BestDirectorAPI(TypeAPI):
#     ...
#
#
# class BestLeadingActorAPI(TypeAPI):
#     ...
#
#
# class BestLeadingActressAPI(TypeAPI):
#     ...
#
#
# class RatingsAPI(TypeAPI):
#     ...
