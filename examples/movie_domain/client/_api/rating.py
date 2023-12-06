from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Rating,
    RatingApply,
    RatingFields,
    RatingList,
    RatingApplyList,
)
from movie_domain.client.data_classes._rating import (
    _RATING_PROPERTIES_BY_FIELD,
    _create_rating_filter,
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
from .rating_score import RatingScoreAPI
from .rating_votes import RatingVotesAPI
from .rating_query import RatingQueryAPI


class RatingAPI(NodeAPI[Rating, RatingApply, RatingList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[RatingApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Rating,
            class_apply_type=RatingApply,
            class_list=RatingList,
            class_apply_list=RatingApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.score = RatingScoreAPI(client, view_id)
        self.votes = RatingVotesAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> RatingQueryAPI[RatingList]:
        """Query starting at ratings.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ratings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for ratings.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_rating_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(RatingList)
        return RatingQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, rating: RatingApply | Sequence[RatingApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) ratings.

        Args:
            rating: Rating or sequence of ratings to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new rating:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import RatingApply
                >>> client = MovieClient()
                >>> rating = RatingApply(external_id="my_rating", ...)
                >>> result = client.rating.apply(rating)

        """
        return self._apply(rating, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more rating.

        Args:
            external_id: External id of the rating to delete.
            space: The space where all the rating are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete rating by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> client.rating.delete("my_rating")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Rating | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> RatingList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Rating | RatingList | None:
        """Retrieve one or more ratings by id(s).

        Args:
            external_id: External id or list of external ids of the ratings.
            space: The space where all the ratings are located.

        Returns:
            The requested ratings.

        Examples:

            Retrieve rating by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> rating = client.rating.retrieve("my_rating")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: RatingFields | Sequence[RatingFields] | None = None,
        group_by: None = None,
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
        property: RatingFields | Sequence[RatingFields] | None = None,
        group_by: RatingFields | Sequence[RatingFields] = None,
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
        property: RatingFields | Sequence[RatingFields] | None = None,
        group_by: RatingFields | Sequence[RatingFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across ratings

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ratings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count ratings in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.rating.aggregate("count", space="my_space")

        """

        filter_ = _create_rating_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _RATING_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: RatingFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for ratings

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ratings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_rating_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _RATING_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> RatingList:
        """List/filter ratings

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of ratings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested ratings

        Examples:

            List ratings and limit to 5:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> ratings = client.rating.list(limit=5)

        """
        filter_ = _create_rating_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
