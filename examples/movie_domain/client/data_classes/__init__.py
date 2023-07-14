from movie_domain.client.data_classes._actors import Actor, ActorApply, ActorList
from movie_domain.client.data_classes._best_directors import BestDirector, BestDirectorApply, BestDirectorList
from movie_domain.client.data_classes._best_leading_actors import (
    BestLeadingActor,
    BestLeadingActorApply,
    BestLeadingActorList,
)
from movie_domain.client.data_classes._best_leading_actresses import (
    BestLeadingActress,
    BestLeadingActressApply,
    BestLeadingActressList,
)
from movie_domain.client.data_classes._directors import Director, DirectorApply, DirectorList
from movie_domain.client.data_classes._movies import Movie, MovieApply, MovieList
from movie_domain.client.data_classes._nominations import Nomination, NominationApply, NominationList
from movie_domain.client.data_classes._persons import Person, PersonApply, PersonList
from movie_domain.client.data_classes._ratings import Rating, RatingApply, RatingList
from movie_domain.client.data_classes._roles import Role, RoleApply, RoleList

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
