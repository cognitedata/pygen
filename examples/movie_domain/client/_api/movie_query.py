from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from movie_domain.client.data_classes import (
    DomainModelApply,
    Movie,
    MovieApply,
    Rating,
    RatingApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .actor_query import ActorQueryAPI
    from .director_query import DirectorQueryAPI


class MovieQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("movie"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[MovieApply], ["*"])]),
                result_cls=Movie,
                max_retrieve_limit=limit,
            )
        )

    def actors(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_rating: bool = False,
    ) -> ActorQueryAPI[T_DomainModelList]:
        """Query along the actor edges of the movie.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of actor edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_rating: Whether to retrieve the rating for each movie or not.

        Returns:
            ActorQueryAPI: The query API for the actor.
        """
        from .actor_query import ActorQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_rating:
            self._query_append_rating(from_)
        return ActorQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def directors(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_rating: bool = False,
    ) -> DirectorQueryAPI[T_DomainModelList]:
        """Query along the director edges of the movie.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of director edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_rating: Whether to retrieve the rating for each movie or not.

        Returns:
            DirectorQueryAPI: The query API for the director.
        """
        from .director_query import DirectorQueryAPI

        from_ = self._builder[-1].name

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
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_rating:
            self._query_append_rating(from_)
        return DirectorQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
        retrieve_rating: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_rating: Whether to retrieve the rating for each movie or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_rating:
            self._query_append_rating(from_)
        return self._query()

    def _query_append_rating(self, from_: str) -> None:
        view_id = self._view_by_write_class[RatingApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("rating"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[MovieApply].as_property_ref("rating"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Rating,
            ),
        )
