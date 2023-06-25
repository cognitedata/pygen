from ._actors import Actor, ActorApply, ActorList
from ._best_directors import BestDirector, BestDirectorApply, BestDirectorList
from ._best_leading_actors import BestLeadingActor, BestLeadingActorApply, BestLeadingActorList
from ._best_leading_actress import BestLeadingActress, BestLeadingActressApply, BestLeadingActressList
from ._directors import Director, DirectorApply, DirectorList
from ._movies import Movie, MovieApply, MovieList
from ._nominations import Nomination, NominationApply, NominationList
from ._persons import Person, PersonApply, PersonList
from ._ratings import Rating, RatingApply, RatingList
from ._roles import Role, RoleApply, RoleList

ActorApply.update_forward_refs(MovieApply=MovieApply, NominationApply=NominationApply, PersonApply=PersonApply)
DirectorApply.update_forward_refs(MovieApply=MovieApply, NominationApply=NominationApply, PersonApply=PersonApply)
MovieApply.update_forward_refs(ActorApply=ActorApply, DirectorApply=DirectorApply)
PersonApply.update_forward_refs(RoleApply=RoleApply)
RoleApply.update_forward_refs(MovieApply=MovieApply, NominationApply=NominationApply, PersonApply=PersonApply)

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
