from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from movie_domain.client.data_classes import (
    Movie,
    MovieApply,
    Actor,
    ActorApply,
    Director,
    DirectorApply,
)
from movie_domain.client.data_classes._movie import (
    _MOVIE_PROPERTIES_BY_FIELD,
)
from movie_domain.client.data_classes._actor import (
    _ACTOR_PROPERTIES_BY_FIELD,
)
from movie_domain.client.data_classes._director import (
    _DIRECTOR_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .actor_query import ActorQueryAPI
    from .director_query import DirectorQueryAPI


class MovieQueryAPI(QueryAPI[T_DomainModelList]):
    def actors(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> ActorQueryAPI[T_DomainModelList]:
        """Query along the actor edges of the movie.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ActorQueryAPI: The query API for the actor.
        """
        from .actor_query import ActorQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.actors"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("actors"),
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
                name=self._builder.next_name("actor"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[ActorApply],
                            list(_ACTOR_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Actor,
                max_retrieve_limit=-1,
            ),
        )
        return ActorQueryAPI(self._client, self._builder, self._view_by_write_class)

    def directors(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> DirectorQueryAPI[T_DomainModelList]:
        """Query along the director edges of the movie.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DirectorQueryAPI: The query API for the director.
        """
        from .director_query import DirectorQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("directors"),
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
                name=self._builder.next_name("director"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[DirectorApply],
                            list(_DIRECTOR_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Director,
                max_retrieve_limit=-1,
            ),
        )
        return DirectorQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_movie: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_movie: Whether to retrieve the movie or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_movie and not self._builder[-1].name.startswith("movie"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("movie"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[MovieApply],
                                list(_MOVIE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Movie,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
