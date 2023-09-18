from ._actor import Actor, ActorApply, ActorList
from ._best_director import BestDirector, BestDirectorApply, BestDirectorList
from ._best_leading_actor import BestLeadingActor, BestLeadingActorApply, BestLeadingActorList
from ._best_leading_actress import BestLeadingActress, BestLeadingActressApply, BestLeadingActressList
from ._director import Director, DirectorApply, DirectorList
from ._movie import Movie, MovieApply, MovieList
from ._nomination import Nomination, NominationApply, NominationList
from ._person import Person, PersonApply, PersonList
from ._rating import Rating, RatingApply, RatingList
from ._role import Role, RoleApply, RoleList

ActorApply.model_rebuild()
DirectorApply.model_rebuild()
MovieApply.model_rebuild()
PersonApply.model_rebuild()
RoleApply.model_rebuild()

__all__ = [
    "Actor",
    "ActorApply",
    "ActorList",
    "BestDirector",
    "BestDirectorApply",
    "BestDirectorList",
    "BestLeadingActor",
    "BestLeadingActorApply",
    "BestLeadingActorList",
    "BestLeadingActress",
    "BestLeadingActressApply",
    "BestLeadingActressList",
    "Director",
    "DirectorApply",
    "DirectorList",
    "Movie",
    "MovieApply",
    "MovieList",
    "Nomination",
    "NominationApply",
    "NominationList",
    "Person",
    "PersonApply",
    "PersonList",
    "Rating",
    "RatingApply",
    "RatingList",
    "Role",
    "RoleApply",
    "RoleList",
]
