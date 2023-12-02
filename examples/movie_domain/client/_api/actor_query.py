from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from movie_domain.client.data_classes import (
    Actor,
    ActorApply,
    Movie,
    MovieApply,
    Nomination,
    NominationApply,
    Person,
    PersonApply,
)
from movie_domain.client.data_classes._actor import (
    _ACTOR_PROPERTIES_BY_FIELD,
)
from movie_domain.client.data_classes._movie import (
    _MOVIE_PROPERTIES_BY_FIELD,
)
from movie_domain.client.data_classes._nomination import (
    _NOMINATION_PROPERTIES_BY_FIELD,
)
from movie_domain.client.data_classes._person import (
    _PERSON_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .movie_query import MovieQueryAPI
    from .nomination_query import NominationQueryAPI


class ActorQueryAPI(QueryAPI[T_DomainModelList]):
    def movies(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> MovieQueryAPI[T_DomainModelList]:
        """Query along the movie edges of the actor.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            MovieQueryAPI: The query API for the movie.
        """
        from .movie_query import MovieQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Role.movies"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("movies"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("movie"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[MovieApply],
                            ["*"],
                        )
                    ]
                ),
                result_cls=Movie,
                max_retrieve_limit=-1,
            ),
        )
        return MovieQueryAPI(self._client, self._builder, self._view_by_write_class)

    def nomination(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> NominationQueryAPI[T_DomainModelList]:
        """Query along the nomination edges of the actor.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            NominationQueryAPI: The query API for the nomination.
        """
        from .nomination_query import NominationQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("nomination"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("nomination"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[NominationApply],
                            list(_NOMINATION_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Nomination,
                max_retrieve_limit=-1,
            ),
        )
        return NominationQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_actor: bool = True,
        retrieve_person: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_actor: Whether to retrieve the actor or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_actor and not self._builder[-1].name.startswith("actor"):
            view_id = self._view_by_write_class[ActorApply]
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("actor"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=dm.filters.HasData(views=[view_id]),
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(view_id, ["*"]),
                        ]
                    ),
                    result_cls=Actor,
                    max_retrieve_limit=-1,
                ),
            )
        if retrieve_person:
            view_id = self._view_by_write_class[PersonApply]
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("person"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=dm.filters.HasData(views=[view_id]),
                        from_=from_,
                        through=self._view_by_write_class[ActorApply].as_property_ref("person"),
                        direction="outwards",
                    ),
                    select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                    max_retrieve_limit=-1,
                    result_cls=Person,
                ),
            )

        return self._query()
