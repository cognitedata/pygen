from ._actor import Actor, ActorApply, ActorList, ActorApplyList
from ._best_director import (
    BestDirector,
    BestDirectorApply,
    BestDirectorList,
    BestDirectorApplyList,
    BestDirectorTextFields,
)
from ._best_leading_actor import (
    BestLeadingActor,
    BestLeadingActorApply,
    BestLeadingActorList,
    BestLeadingActorApplyList,
    BestLeadingActorTextFields,
)
from ._best_leading_actress import (
    BestLeadingActress,
    BestLeadingActressApply,
    BestLeadingActressList,
    BestLeadingActressApplyList,
    BestLeadingActressTextFields,
)
from ._director import Director, DirectorApply, DirectorList, DirectorApplyList
from ._movie import Movie, MovieApply, MovieList, MovieApplyList, MovieTextFields
from ._nomination import Nomination, NominationApply, NominationList, NominationApplyList, NominationTextFields
from ._person import Person, PersonApply, PersonList, PersonApplyList, PersonFields, PersonTextFields
from ._rating import Rating, RatingApply, RatingList, RatingApplyList, RatingTextFields
from ._role import Role, RoleApply, RoleList, RoleApplyList

ActorApply.model_rebuild()
DirectorApply.model_rebuild()
MovieApply.model_rebuild()
PersonApply.model_rebuild()
RoleApply.model_rebuild()

__all__ = [
    "Actor",
    "ActorApply",
    "ActorList",
    "ActorApplyList",
    "BestDirector",
    "BestDirectorApply",
    "BestDirectorList",
    "BestDirectorApplyList",
    "BestDirectorTextFields",
    "BestLeadingActor",
    "BestLeadingActorApply",
    "BestLeadingActorList",
    "BestLeadingActorApplyList",
    "BestLeadingActorTextFields",
    "BestLeadingActress",
    "BestLeadingActressApply",
    "BestLeadingActressList",
    "BestLeadingActressApplyList",
    "BestLeadingActressTextFields",
    "Director",
    "DirectorApply",
    "DirectorList",
    "DirectorApplyList",
    "Movie",
    "MovieApply",
    "MovieList",
    "MovieApplyList",
    "MovieTextFields",
    "Nomination",
    "NominationApply",
    "NominationList",
    "NominationApplyList",
    "NominationTextFields",
    "Person",
    "PersonApply",
    "PersonList",
    "PersonApplyList",
    "PersonFields",
    "PersonTextFields",
    "Rating",
    "RatingApply",
    "RatingList",
    "RatingApplyList",
    "RatingTextFields",
    "Role",
    "RoleApply",
    "RoleList",
    "RoleApplyList",
]
