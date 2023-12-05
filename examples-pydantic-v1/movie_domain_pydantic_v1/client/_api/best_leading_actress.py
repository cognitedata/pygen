from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    BestLeadingActress,
    BestLeadingActressApply,
    BestLeadingActressFields,
    BestLeadingActressList,
    BestLeadingActressApplyList,
    BestLeadingActressTextFields,
)
from movie_domain_pydantic_v1.client.data_classes._best_leading_actress import (
    _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD,
    _create_best_leading_actress_filter,
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
from .best_leading_actress_query import BestLeadingActressQueryAPI


class BestLeadingActressAPI(NodeAPI[BestLeadingActress, BestLeadingActressApply, BestLeadingActressList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BestLeadingActressApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BestLeadingActress,
            class_apply_type=BestLeadingActressApply,
            class_list=BestLeadingActressList,
            class_apply_list=BestLeadingActressApplyList,
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BestLeadingActressQueryAPI[BestLeadingActressList]:
        """Query starting at best leading actresses.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for best leading actresses.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_best_leading_actress_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BestLeadingActressList)
        return BestLeadingActressQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, best_leading_actress: BestLeadingActressApply | Sequence[BestLeadingActressApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) best leading actresses.

        Args:
            best_leading_actress: Best leading actress or sequence of best leading actresses to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new best_leading_actress:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> from movie_domain_pydantic_v1.client.data_classes import BestLeadingActressApply
                >>> client = MovieClient()
                >>> best_leading_actress = BestLeadingActressApply(external_id="my_best_leading_actress", ...)
                >>> result = client.best_leading_actress.apply(best_leading_actress)

        """
        return self._apply(best_leading_actress, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more best leading actress.

        Args:
            external_id: External id of the best leading actress to delete.
            space: The space where all the best leading actress are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete best_leading_actress by id:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> client.best_leading_actress.delete("my_best_leading_actress")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> BestLeadingActress | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> BestLeadingActressList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> BestLeadingActress | BestLeadingActressList | None:
        """Retrieve one or more best leading actresses by id(s).

        Args:
            external_id: External id or list of external ids of the best leading actresses.
            space: The space where all the best leading actresses are located.

        Returns:
            The requested best leading actresses.

        Examples:

            Retrieve best_leading_actress by id:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> best_leading_actress = client.best_leading_actress.retrieve("my_best_leading_actress")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BestLeadingActressList:
        """Search best leading actresses

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results best leading actresses matching the query.

        Examples:

           Search for 'my_best_leading_actress' in all text properties:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> best_leading_actresses = client.best_leading_actress.search('my_best_leading_actress')

        """
        filter_ = _create_best_leading_actress_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
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
        property: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        group_by: BestLeadingActressFields | Sequence[BestLeadingActressFields] = None,
        query: str | None = None,
        search_properties: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
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
        property: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        group_by: BestLeadingActressFields | Sequence[BestLeadingActressFields] | None = None,
        query: str | None = None,
        search_property: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across best leading actresses

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
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count best leading actresses in space `my_space`:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.best_leading_actress.aggregate("count", space="my_space")

        """

        filter_ = _create_best_leading_actress_filter(
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
            _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BestLeadingActressFields,
        interval: float,
        query: str | None = None,
        search_property: BestLeadingActressTextFields | Sequence[BestLeadingActressTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for best leading actresses

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
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_best_leading_actress_filter(
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
            _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD,
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
    ) -> BestLeadingActressList:
        """List/filter best leading actresses

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of best leading actresses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested best leading actresses

        Examples:

            List best leading actresses and limit to 5:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> best_leading_actresses = client.best_leading_actress.list(limit=5)

        """
        filter_ = _create_best_leading_actress_filter(
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
