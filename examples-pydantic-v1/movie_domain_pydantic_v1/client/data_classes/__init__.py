from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    ResourcesApplyResult,
)
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
from ._rating import Rating, RatingApply, RatingApplyList, RatingFields, RatingList
from ._role import Role, RoleApply, RoleApplyList, RoleFields, RoleList


Actor.update_forward_refs(
    Movie=Movie,
    Nomination=Nomination,
    Person=Person,
)
ActorApply.update_forward_refs(
    MovieApply=MovieApply,
    NominationApply=NominationApply,
    PersonApply=PersonApply,
)

Director.update_forward_refs(
    Movie=Movie,
    Nomination=Nomination,
    Person=Person,
)
DirectorApply.update_forward_refs(
    MovieApply=MovieApply,
    NominationApply=NominationApply,
    PersonApply=PersonApply,
)

Movie.update_forward_refs(
    Actor=Actor,
    Director=Director,
    Rating=Rating,
)
MovieApply.update_forward_refs(
    ActorApply=ActorApply,
    DirectorApply=DirectorApply,
    RatingApply=RatingApply,
)

Person.update_forward_refs(
    Role=Role,
)
PersonApply.update_forward_refs(
    RoleApply=RoleApply,
)

Role.update_forward_refs(
    Movie=Movie,
    Nomination=Nomination,
    Person=Person,
)
RoleApply.update_forward_refs(
    MovieApply=MovieApply,
    NominationApply=NominationApply,
    PersonApply=PersonApply,
)

__all__ = [
    "ResourcesApply",
    "DomainModel",
    "DomainModelApply",
    "DomainModelList",
    "DomainRelationApply",
    "ResourcesApplyResult",
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
    "Role",
    "RoleApply",
    "RoleList",
    "RoleApplyList",
    "RoleFields",
]
