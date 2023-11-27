from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    PygenPool,
    PygenPoolApply,
    PygenPoolFields,
    PygenPoolList,
    PygenPoolApplyList,
    PygenPoolTextFields,
)
from markets.client.data_classes._pygen_pool import (
    _PYGENPOOL_PROPERTIES_BY_FIELD,
    _create_pygen_pool_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .pygen_pool_query import PygenPoolQueryAPI


class PygenPoolAPI(NodeAPI[PygenPool, PygenPoolApply, PygenPoolList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PygenPoolApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PygenPool,
            class_apply_type=PygenPoolApply,
            class_list=PygenPoolList,
            class_apply_list=PygenPoolApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenPoolQueryAPI[PygenPoolList]:
        """Query starting at pygen pools.

        Args:
            min_day_of_week: The minimum value of the day of week to filter on.
            max_day_of_week: The maximum value of the day of week to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for pygen pools.

        """
        filter_ = _create_pygen_pool_filter(
            self._view_id,
            min_day_of_week,
            max_day_of_week,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            PygenPoolList,
            [
                QueryStep(
                    name="pygen_pool",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_PYGENPOOL_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=PygenPool,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return PygenPoolQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, pygen_pool: PygenPoolApply | Sequence[PygenPoolApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) pygen pools.

        Args:
            pygen_pool: Pygen pool or sequence of pygen pools to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new pygen_pool:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import PygenPoolApply
                >>> client = MarketClient()
                >>> pygen_pool = PygenPoolApply(external_id="my_pygen_pool", ...)
                >>> result = client.pygen_pool.apply(pygen_pool)

        """
        return self._apply(pygen_pool, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more pygen pool.

        Args:
            external_id: External id of the pygen pool to delete.
            space: The space where all the pygen pool are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete pygen_pool by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> client.pygen_pool.delete("my_pygen_pool")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> PygenPool:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> PygenPoolList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "market") -> PygenPool | PygenPoolList:
        """Retrieve one or more pygen pools by id(s).

        Args:
            external_id: External id or list of external ids of the pygen pools.
            space: The space where all the pygen pools are located.

        Returns:
            The requested pygen pools.

        Examples:

            Retrieve pygen_pool by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_pool = client.pygen_pool.retrieve("my_pygen_pool")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PygenPoolTextFields | Sequence[PygenPoolTextFields] | None = None,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenPoolList:
        """Search pygen pools

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_day_of_week: The minimum value of the day of week to filter on.
            max_day_of_week: The maximum value of the day of week to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results pygen pools matching the query.

        Examples:

           Search for 'my_pygen_pool' in all text properties:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_pools = client.pygen_pool.search('my_pygen_pool')

        """
        filter_ = _create_pygen_pool_filter(
            self._view_id,
            min_day_of_week,
            max_day_of_week,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PYGENPOOL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PygenPoolFields | Sequence[PygenPoolFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PygenPoolTextFields | Sequence[PygenPoolTextFields] | None = None,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: PygenPoolFields | Sequence[PygenPoolFields] | None = None,
        group_by: PygenPoolFields | Sequence[PygenPoolFields] = None,
        query: str | None = None,
        search_properties: PygenPoolTextFields | Sequence[PygenPoolTextFields] | None = None,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: PygenPoolFields | Sequence[PygenPoolFields] | None = None,
        group_by: PygenPoolFields | Sequence[PygenPoolFields] | None = None,
        query: str | None = None,
        search_property: PygenPoolTextFields | Sequence[PygenPoolTextFields] | None = None,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across pygen pools

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_day_of_week: The minimum value of the day of week to filter on.
            max_day_of_week: The maximum value of the day of week to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count pygen pools in space `my_space`:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.pygen_pool.aggregate("count", space="my_space")

        """

        filter_ = _create_pygen_pool_filter(
            self._view_id,
            min_day_of_week,
            max_day_of_week,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PYGENPOOL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PygenPoolFields,
        interval: float,
        query: str | None = None,
        search_property: PygenPoolTextFields | Sequence[PygenPoolTextFields] | None = None,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for pygen pools

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_day_of_week: The minimum value of the day of week to filter on.
            max_day_of_week: The maximum value of the day of week to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_pygen_pool_filter(
            self._view_id,
            min_day_of_week,
            max_day_of_week,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PYGENPOOL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenPoolList:
        """List/filter pygen pools

        Args:
            min_day_of_week: The minimum value of the day of week to filter on.
            max_day_of_week: The maximum value of the day of week to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested pygen pools

        Examples:

            List pygen pools and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_pools = client.pygen_pool.list(limit=5)

        """
        filter_ = _create_pygen_pool_filter(
            self._view_id,
            min_day_of_week,
            max_day_of_week,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
