from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from movie_domain.client.data_classes import (
    Movie,
    MovieApply,
)
from movie_domain.client.data_classes._movie import (
    _MOVIE_PROPERTIES_BY_FIELD,
)
if TYPE_CHECKING:

    from actor_query import ActorQueryAPI

    from director_query import DirectorQueryAPI



class MovieQueryAPI(QueryAPI[T_DomainModelList]):
    def actors(
        self,
        limit: int | None = None,
    ) -> ActorQueryAPI[T_DomainModelList]:
        """Query along the actor edges of the movie.

        Args:
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ActorQueryAPI: The query API for the actor.
        """

    from actor_query import ActorQueryAPI

        f = dm.filters
        edge_view = self._view_by_write_class[ActorApply]
        edge_filter = _create_actor_filter(
            edge_view,
            None,
            None,
            None,
            None,
            f.Equals(
                ["edge", "type"],
                {"space": "REPLACE", "externalId": "REPLACE"},
            ),
        )
        self._builder.append(
            QueryStep(
                name="actors",
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                max_retrieve_limit=limit,
            )
        )
        return ActorQueryAPI(self._client, self._builder, self._view_by_write_class)

    def directors(
        self,
        limit: int | None = None,
    ) -> DirectorQueryAPI[T_DomainModelList]:
        """Query along the director edges of the movie.

        Args:
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DirectorQueryAPI: The query API for the director.
        """

    from director_query import DirectorQueryAPI

        f = dm.filters
        edge_view = self._view_by_write_class[DirectorApply]
        edge_filter = _create_director_filter(
            edge_view,
            None,
            None,
            None,
            None,
            f.Equals(
                ["edge", "type"],
                {"space": "REPLACE", "externalId": "REPLACE"},
            ),
        )
        self._builder.append(
            QueryStep(
                name="directors",
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                max_retrieve_limit=limit,
            )
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
                    name="movie",
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