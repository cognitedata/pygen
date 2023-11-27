from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Market,
    MarketApply,
    MarketFields,
    MarketList,
    MarketApplyList,
    MarketTextFields,
)
from markets_pydantic_v1.client.data_classes._market import (
    _MARKET_PROPERTIES_BY_FIELD,
    _create_market_filter,
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
from .market_query import MarketQueryAPI


class MarketAPI(NodeAPI[Market, MarketApply, MarketList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MarketApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Market,
            class_apply_type=MarketApply,
            class_list=MarketList,
            class_apply_list=MarketApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MarketQueryAPI[MarketList]:
        """Query starting at markets.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of markets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for markets.

        """
        filter_ = _create_market_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            MarketList,
            [
                QueryStep(
                    name="market",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_MARKET_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Market,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return MarketQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, market: MarketApply | Sequence[MarketApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) markets.

        Args:
            market: Market or sequence of markets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new market:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> from markets_pydantic_v1.client.data_classes import MarketApply
                >>> client = MarketClient()
                >>> market = MarketApply(external_id="my_market", ...)
                >>> result = client.market.apply(market)

        """
        return self._apply(market, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more market.

        Args:
            external_id: External id of the market to delete.
            space: The space where all the market are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete market by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> client.market.delete("my_market")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Market:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> MarketList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "market") -> Market | MarketList:
        """Retrieve one or more markets by id(s).

        Args:
            external_id: External id or list of external ids of the markets.
            space: The space where all the markets are located.

        Returns:
            The requested markets.

        Examples:

            Retrieve market by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> market = client.market.retrieve("my_market")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: MarketTextFields | Sequence[MarketTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketList:
        """Search markets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of markets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results markets matching the query.

        Examples:

           Search for 'my_market' in all text properties:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> markets = client.market.search('my_market')

        """
        filter_ = _create_market_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _MARKET_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MarketFields | Sequence[MarketFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MarketTextFields | Sequence[MarketTextFields] | None = None,
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
        property: MarketFields | Sequence[MarketFields] | None = None,
        group_by: MarketFields | Sequence[MarketFields] = None,
        query: str | None = None,
        search_properties: MarketTextFields | Sequence[MarketTextFields] | None = None,
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
        property: MarketFields | Sequence[MarketFields] | None = None,
        group_by: MarketFields | Sequence[MarketFields] | None = None,
        query: str | None = None,
        search_property: MarketTextFields | Sequence[MarketTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across markets

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of markets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count markets in space `my_space`:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.market.aggregate("count", space="my_space")

        """

        filter_ = _create_market_filter(
            self._view_id,
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
            _MARKET_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MarketFields,
        interval: float,
        query: str | None = None,
        search_property: MarketTextFields | Sequence[MarketTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for markets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of markets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_market_filter(
            self._view_id,
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
            _MARKET_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketList:
        """List/filter markets

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of markets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested markets

        Examples:

            List markets and limit to 5:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> markets = client.market.list(limit=5)

        """
        filter_ = _create_market_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
