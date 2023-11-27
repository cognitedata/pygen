from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CogBid,
    CogBidApply,
    CogBidFields,
    CogBidList,
    CogBidApplyList,
    CogBidTextFields,
)
from markets_pydantic_v1.client.data_classes._cog_bid import (
    _COGBID_PROPERTIES_BY_FIELD,
    _create_cog_bid_filter,
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
from .cog_bid_query import CogBidQueryAPI


class CogBidAPI(NodeAPI[CogBid, CogBidApply, CogBidList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CogBidApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogBid,
            class_apply_type=CogBidApply,
            class_list=CogBidList,
            class_apply_list=CogBidApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogBidQueryAPI[CogBidList]:
        """Query starting at cog bids.

        Args:
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_quantity: The minimum value of the quantity to filter on.
            max_quantity: The maximum value of the quantity to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cog bids.

        """
        filter_ = _create_cog_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            min_price,
            max_price,
            price_area,
            price_area_prefix,
            min_quantity,
            max_quantity,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            CogBidList,
            [
                QueryStep(
                    name="cog_bid",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_COGBID_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=CogBid,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return CogBidQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, cog_bid: CogBidApply | Sequence[CogBidApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) cog bids.

        Args:
            cog_bid: Cog bid or sequence of cog bids to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cog_bid:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> from markets_pydantic_v1.client.data_classes import CogBidApply
                >>> client = MarketClient()
                >>> cog_bid = CogBidApply(external_id="my_cog_bid", ...)
                >>> result = client.cog_bid.apply(cog_bid)

        """
        return self._apply(cog_bid, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more cog bid.

        Args:
            external_id: External id of the cog bid to delete.
            space: The space where all the cog bid are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cog_bid by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> client.cog_bid.delete("my_cog_bid")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> CogBid | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> CogBidList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "market") -> CogBid | CogBidList | None:
        """Retrieve one or more cog bids by id(s).

        Args:
            external_id: External id or list of external ids of the cog bids.
            space: The space where all the cog bids are located.

        Returns:
            The requested cog bids.

        Examples:

            Retrieve cog_bid by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_bid = client.cog_bid.retrieve("my_cog_bid")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: CogBidTextFields | Sequence[CogBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogBidList:
        """Search cog bids

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_quantity: The minimum value of the quantity to filter on.
            max_quantity: The maximum value of the quantity to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results cog bids matching the query.

        Examples:

           Search for 'my_cog_bid' in all text properties:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_bids = client.cog_bid.search('my_cog_bid')

        """
        filter_ = _create_cog_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            min_price,
            max_price,
            price_area,
            price_area_prefix,
            min_quantity,
            max_quantity,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _COGBID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CogBidFields | Sequence[CogBidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CogBidTextFields | Sequence[CogBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
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
        property: CogBidFields | Sequence[CogBidFields] | None = None,
        group_by: CogBidFields | Sequence[CogBidFields] = None,
        query: str | None = None,
        search_properties: CogBidTextFields | Sequence[CogBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
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
        property: CogBidFields | Sequence[CogBidFields] | None = None,
        group_by: CogBidFields | Sequence[CogBidFields] | None = None,
        query: str | None = None,
        search_property: CogBidTextFields | Sequence[CogBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cog bids

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_quantity: The minimum value of the quantity to filter on.
            max_quantity: The maximum value of the quantity to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cog bids in space `my_space`:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.cog_bid.aggregate("count", space="my_space")

        """

        filter_ = _create_cog_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            min_price,
            max_price,
            price_area,
            price_area_prefix,
            min_quantity,
            max_quantity,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _COGBID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CogBidFields,
        interval: float,
        query: str | None = None,
        search_property: CogBidTextFields | Sequence[CogBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cog bids

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_quantity: The minimum value of the quantity to filter on.
            max_quantity: The maximum value of the quantity to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cog_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            min_price,
            max_price,
            price_area,
            price_area_prefix,
            min_quantity,
            max_quantity,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _COGBID_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_quantity: int | None = None,
        max_quantity: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogBidList:
        """List/filter cog bids

        Args:
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_quantity: The minimum value of the quantity to filter on.
            max_quantity: The maximum value of the quantity to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cog bids

        Examples:

            List cog bids and limit to 5:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> cog_bids = client.cog_bid.list(limit=5)

        """
        filter_ = _create_cog_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            min_price,
            max_price,
            price_area,
            price_area_prefix,
            min_quantity,
            max_quantity,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
