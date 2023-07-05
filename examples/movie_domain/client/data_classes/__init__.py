from ._actors import Actor, ActorApply, ActorList
from ._best_directors import BestDirector, BestDirectorApply, BestDirectorList
from ._best_leading_actors import BestLeadingActor, BestLeadingActorApply, BestLeadingActorList
from ._best_leading_actresses import BestLeadingActress, BestLeadingActressApply, BestLeadingActressList
from ._directors import Director, DirectorApply, DirectorList
from ._movies import Movie, MovieApply, MovieList
from ._nominations import Nomination, NominationApply, NominationList
from ._persons import Person, PersonApply, PersonList
from ._ratings import Rating, RatingApply, RatingList
from ._roles import Role, RoleApply, RoleList

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
