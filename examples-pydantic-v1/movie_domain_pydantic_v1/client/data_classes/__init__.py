from ._actor import Actor, ActorApply, ActorList, ActorApplyList
from ._best_director import BestDirector, BestDirectorApply, BestDirectorList, BestDirectorApplyList
from ._best_leading_actor import (
    BestLeadingActor,
    BestLeadingActorApply,
    BestLeadingActorList,
    BestLeadingActorApplyList,
)
from ._best_leading_actress import (
    BestLeadingActress,
    BestLeadingActressApply,
    BestLeadingActressList,
    BestLeadingActressApplyList,
)
from ._director import Director, DirectorApply, DirectorList, DirectorApplyList
from ._movie import Movie, MovieApply, MovieList, MovieApplyList
from ._nomination import Nomination, NominationApply, NominationList, NominationApplyList
from ._person import Person, PersonApply, PersonList, PersonApplyList
from ._rating import Rating, RatingApply, RatingList, RatingApplyList
from ._role import Role, RoleApply, RoleList, RoleApplyList

ActorApply.update_forward_refs(
    MovieApply=MovieApply,
    NominationApply=NominationApply,
    PersonApply=PersonApply,
)
DirectorApply.update_forward_refs(
    MovieApply=MovieApply,
    NominationApply=NominationApply,
    PersonApply=PersonApply,
)
MovieApply.update_forward_refs(
    ActorApply=ActorApply,
    DirectorApply=DirectorApply,
    RatingApply=RatingApply,
)
PersonApply.update_forward_refs(
    RoleApply=RoleApply,
)
RoleApply.update_forward_refs(
    MovieApply=MovieApply,
    NominationApply=NominationApply,
    PersonApply=PersonApply,
)

__all__ = [
    "Actor",
    "ActorApply",
    "ActorList",
    "ActorApplyList",
    "BestDirector",
    "BestDirectorApply",
    "BestDirectorList",
    "BestDirectorApplyList",
    "BestLeadingActor",
    "BestLeadingActorApply",
    "BestLeadingActorList",
    "BestLeadingActorApplyList",
    "BestLeadingActress",
    "BestLeadingActressApply",
    "BestLeadingActressList",
    "BestLeadingActressApplyList",
    "Director",
    "DirectorApply",
    "DirectorList",
    "DirectorApplyList",
    "Movie",
    "MovieApply",
    "MovieList",
    "MovieApplyList",
    "Nomination",
    "NominationApply",
    "NominationList",
    "NominationApplyList",
    "Person",
    "PersonApply",
    "PersonList",
    "PersonApplyList",
    "Rating",
    "RatingApply",
    "RatingList",
    "RatingApplyList",
    "Role",
    "RoleApply",
    "RoleList",
    "RoleApplyList",
]
