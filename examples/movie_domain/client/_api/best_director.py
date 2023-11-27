from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    BestDirector,
    BestDirectorApply,
    BestDirectorFields,
    BestDirectorList,
    BestDirectorTextFields,
)
from movie_domain.client.data_classes._best_director import (
    _BESTDIRECTOR_PROPERTIES_BY_FIELD,
    _create_best_director_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .best_director_query import BestDirectorQueryAPI


class BestDirectorAPI(NodeAPI[BestDirector, BestDirectorApply, BestDirectorList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BestDirectorApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BestDirector,
            class_apply_type=BestDirectorApply,
            class_list=BestDirectorList,
            class_apply_list=BestDirectorApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BestDirectorQueryAPI[BestDirectorList]:
        """Query starting at best directors.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for best directors.

        """
        filter_ = _create_best_director_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            BestDirectorList,
            [
                QueryStep(
                    name="best_director",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_BESTDIRECTOR_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=BestDirector,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return BestDirectorQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, best_director: BestDirectorApply | Sequence[BestDirectorApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) best directors.

        Args:
            best_director: Best director or sequence of best directors to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new best_director:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import BestDirectorApply
                >>> client = MovieClient()
                >>> best_director = BestDirectorApply(external_id="my_best_director", ...)
                >>> result = client.best_director.apply(best_director)

        """
        return self._apply(best_director, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more best director.

        Args:
            external_id: External id of the best director to delete.
            space: The space where all the best director are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete best_director by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> client.best_director.delete("my_best_director")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> BestDirector:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> BestDirectorList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> BestDirector | BestDirectorList:
        """Retrieve one or more best directors by id(s).

        Args:
            external_id: External id or list of external ids of the best directors.
            space: The space where all the best directors are located.

        Returns:
            The requested best directors.

        Examples:

            Retrieve best_director by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> best_director = client.best_director.retrieve("my_best_director")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: BestDirectorTextFields | Sequence[BestDirectorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BestDirectorList:
        """Search best directors

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results best directors matching the query.

        Examples:

           Search for 'my_best_director' in all text properties:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> best_directors = client.best_director.search('my_best_director')

        """
        filter_ = _create_best_director_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BESTDIRECTOR_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BestDirectorFields | Sequence[BestDirectorFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BestDirectorTextFields | Sequence[BestDirectorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
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
        property: BestDirectorFields | Sequence[BestDirectorFields] | None = None,
        group_by: BestDirectorFields | Sequence[BestDirectorFields] = None,
        query: str | None = None,
        search_properties: BestDirectorTextFields | Sequence[BestDirectorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
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
        property: BestDirectorFields | Sequence[BestDirectorFields] | None = None,
        group_by: BestDirectorFields | Sequence[BestDirectorFields] | None = None,
        query: str | None = None,
        search_property: BestDirectorTextFields | Sequence[BestDirectorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across best directors

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count best directors in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.best_director.aggregate("count", space="my_space")

        """

        filter_ = _create_best_director_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BESTDIRECTOR_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BestDirectorFields,
        interval: float,
        query: str | None = None,
        search_property: BestDirectorTextFields | Sequence[BestDirectorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for best directors

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_best_director_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BESTDIRECTOR_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BestDirectorList:
        """List/filter best directors

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best directors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested best directors

        Examples:

            List best directors and limit to 5:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> best_directors = client.best_director.list(limit=5)

        """
        filter_ = _create_best_director_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
