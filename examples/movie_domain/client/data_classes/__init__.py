from ._actor import Actor, ActorApply, ActorApplyList, ActorFields, ActorList
from ._best_director import (
    BestDirector,
    BestDirectorApply,
    BestDirectorApplyList,
    BestDirectorFields,
    BestDirectorList,
    BestDirectorTextFields,
)
from ._best_leading_actor import (
    BestLeadingActor,
    BestLeadingActorApply,
    BestLeadingActorApplyList,
    BestLeadingActorFields,
    BestLeadingActorList,
    BestLeadingActorTextFields,
)
from ._best_leading_actress import (
    BestLeadingActress,
    BestLeadingActressApply,
    BestLeadingActressApplyList,
    BestLeadingActressFields,
    BestLeadingActressList,
    BestLeadingActressTextFields,
)
from ._director import Director, DirectorApply, DirectorApplyList, DirectorFields, DirectorList
from ._movie import Movie, MovieApply, MovieApplyList, MovieFields, MovieList, MovieTextFields
from ._nomination import (
    Nomination,
    NominationApply,
    NominationApplyList,
    NominationFields,
    NominationList,
    NominationTextFields,
)
from ._person import Person, PersonApply, PersonApplyList, PersonFields, PersonList, PersonTextFields
from ._rating import Rating, RatingApply, RatingApplyList, RatingFields, RatingList, RatingTextFields
from ._role import Role, RoleApply, RoleApplyList, RoleFields, RoleList

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
    "ActorFields",
    "BestDirector",
    "BestDirectorApply",
    "BestDirectorList",
    "BestDirectorApplyList",
    "BestDirectorFields",
    "BestDirectorTextFields",
    "BestLeadingActor",
    "BestLeadingActorApply",
    "BestLeadingActorList",
    "BestLeadingActorApplyList",
    "BestLeadingActorFields",
    "BestLeadingActorTextFields",
    "BestLeadingActress",
    "BestLeadingActressApply",
    "BestLeadingActressList",
    "BestLeadingActressApplyList",
    "BestLeadingActressFields",
    "BestLeadingActressTextFields",
    "Director",
    "DirectorApply",
    "DirectorList",
    "DirectorApplyList",
    "DirectorFields",
    "Movie",
    "MovieApply",
    "MovieList",
    "MovieApplyList",
    "MovieFields",
    "MovieTextFields",
    "Nomination",
    "NominationApply",
    "NominationList",
    "NominationApplyList",
    "NominationFields",
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
    "RatingFields",
    "RatingTextFields",
    "Role",
    "RoleApply",
    "RoleList",
    "RoleApplyList",
    "RoleFields",
]
