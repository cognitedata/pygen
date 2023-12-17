from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from movie_domain.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Director,
    DirectorApply,
    DirectorFields,
    DirectorList,
    DirectorApplyList,
)
from movie_domain.client.data_classes._director import (
    _DIRECTOR_PROPERTIES_BY_FIELD,
    _create_director_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .director_movies import DirectorMoviesAPI
from .director_nomination import DirectorNominationAPI
from .director_query import DirectorQueryAPI


class DirectorAPI(NodeAPI[Director, DirectorApply, DirectorList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[DirectorApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Director,
            class_apply_type=DirectorApply,
            class_list=DirectorList,
            class_apply_list=DirectorApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.movies_edge = DirectorMoviesAPI(client)
        self.nomination_edge = DirectorNominationAPI(client)

    def __call__(
        self,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> DirectorQueryAPI[DirectorList]:
        """Query starting at directors.

        Args:
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for directors.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_director_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(DirectorList)
        return DirectorQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, director: DirectorApply | Sequence[DirectorApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) directors.

        Note: This method iterates through all nodes and timeseries linked to director and creates them including the edges
        between the nodes. For example, if any of `movies` or `nomination` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            director: Director or sequence of directors to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new director:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import DirectorApply
                >>> client = MovieClient()
                >>> director = DirectorApply(external_id="my_director", ...)
                >>> result = client.director.apply(director)

        """
        return self._apply(director, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more director.

        Args:
            external_id: External id of the director to delete.
            space: The space where all the director are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete director by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> client.director.delete("my_director")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Director | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> DirectorList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Director | DirectorList | None:
        """Retrieve one or more directors by id(s).

        Args:
            external_id: External id or list of external ids of the directors.
            space: The space where all the directors are located.

        Returns:
            The requested directors.

        Examples:

            Retrieve director by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> director = client.director.retrieve("my_director")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (
                    self.movies_edge,
                    "movies",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors"),
                ),
                (
                    self.nomination_edge,
                    "nomination",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
                ),
            ],
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DirectorFields | Sequence[DirectorFields] | None = None,
        group_by: None = None,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DirectorFields | Sequence[DirectorFields] | None = None,
        group_by: DirectorFields | Sequence[DirectorFields] = None,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DirectorFields | Sequence[DirectorFields] | None = None,
        group_by: DirectorFields | Sequence[DirectorFields] | None = None,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across directors

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count directors in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.director.aggregate("count", space="my_space")

        """

        filter_ = _create_director_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _DIRECTOR_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DirectorFields,
        interval: float,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for directors

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_director_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _DIRECTOR_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DirectorList:
        """List/filter directors

        Args:
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `movies` or `nomination` external ids for the directors. Defaults to True.

        Returns:
            List of requested directors

        Examples:

            List directors and limit to 5:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> directors = client.director.list(limit=5)

        """
        filter_ = _create_director_filter(
            self._view_id,
            person,
            won_oscar,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (
                    self.movies_edge,
                    "movies",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors"),
                ),
                (
                    self.nomination_edge,
                    "nomination",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
                ),
            ],
        )
