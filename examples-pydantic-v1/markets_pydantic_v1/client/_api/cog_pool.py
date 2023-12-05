from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CogPool,
    CogPoolApply,
    CogPoolFields,
    CogPoolList,
    CogPoolApplyList,
    CogPoolTextFields,
)
from markets_pydantic_v1.client.data_classes._cog_pool import (
    _COGPOOL_PROPERTIES_BY_FIELD,
    _create_cog_pool_filter,
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
from .cog_pool_query import CogPoolQueryAPI


class CogPoolAPI(NodeAPI[CogPool, CogPoolApply, CogPoolList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CogPoolApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogPool,
            class_apply_type=CogPoolApply,
            class_list=CogPoolList,
            class_apply_list=CogPoolApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogPoolQueryAPI[CogPoolList]:
        """Query starting at cog pools.

        Args:
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cog pools.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cog_pool_filter(
            self._view_id,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            name,
            name_prefix,
            time_unit,
            time_unit_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(CogPoolList)
        return CogPoolQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, cog_pool: CogPoolApply | Sequence[CogPoolApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) cog pools.

        Args:
            cog_pool: Cog pool or sequence of cog pools to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cog_pool:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> from markets_pydantic_v1.client.data_classes import CogPoolApply
                >>> client = MarketClient()
                >>> cog_pool = CogPoolApply(external_id="my_cog_pool", ...)
                >>> result = client.cog_pool.apply(cog_pool)

        """
        return self._apply(cog_pool, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more cog pool.

        Args:
            external_id: External id of the cog pool to delete.
            space: The space where all the cog pool are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cog_pool by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> client.cog_pool.delete("my_cog_pool")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> CogPool | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> CogPoolList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "market") -> CogPool | CogPoolList | None:
        """Retrieve one or more cog pools by id(s).

        Args:
            external_id: External id or list of external ids of the cog pools.
            space: The space where all the cog pools are located.

        Returns:
            The requested cog pools.

        Examples:

            Retrieve cog_pool by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_pool = client.cog_pool.retrieve("my_cog_pool")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: CogPoolTextFields | Sequence[CogPoolTextFields] | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogPoolList:
        """Search cog pools

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results cog pools matching the query.

        Examples:

           Search for 'my_cog_pool' in all text properties:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_pools = client.cog_pool.search('my_cog_pool')

        """
        filter_ = _create_cog_pool_filter(
            self._view_id,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            name,
            name_prefix,
            time_unit,
            time_unit_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _COGPOOL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CogPoolFields | Sequence[CogPoolFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CogPoolTextFields | Sequence[CogPoolTextFields] | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
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
        property: CogPoolFields | Sequence[CogPoolFields] | None = None,
        group_by: CogPoolFields | Sequence[CogPoolFields] = None,
        query: str | None = None,
        search_properties: CogPoolTextFields | Sequence[CogPoolTextFields] | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
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
        property: CogPoolFields | Sequence[CogPoolFields] | None = None,
        group_by: CogPoolFields | Sequence[CogPoolFields] | None = None,
        query: str | None = None,
        search_property: CogPoolTextFields | Sequence[CogPoolTextFields] | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cog pools

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cog pools in space `my_space`:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.cog_pool.aggregate("count", space="my_space")

        """

        filter_ = _create_cog_pool_filter(
            self._view_id,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            name,
            name_prefix,
            time_unit,
            time_unit_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _COGPOOL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CogPoolFields,
        interval: float,
        query: str | None = None,
        search_property: CogPoolTextFields | Sequence[CogPoolTextFields] | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cog pools

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cog_pool_filter(
            self._view_id,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            name,
            name_prefix,
            time_unit,
            time_unit_prefix,
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
            _COGPOOL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogPoolList:
        """List/filter cog pools

        Args:
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog pools to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cog pools

        Examples:

            List cog pools and limit to 5:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_pools = client.cog_pool.list(limit=5)

        """
        filter_ = _create_cog_pool_filter(
            self._view_id,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            name,
            name_prefix,
            time_unit,
            time_unit_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
