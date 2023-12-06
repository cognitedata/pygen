from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Bid,
    BidApply,
    BidFields,
    BidList,
    BidApplyList,
    BidTextFields,
)
from markets.client.data_classes._bid import (
    _BID_PROPERTIES_BY_FIELD,
    _create_bid_filter,
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
from .bid_query import BidQueryAPI


class BidAPI(NodeAPI[Bid, BidApply, BidList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BidApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Bid,
            class_apply_type=BidApply,
            class_list=BidList,
            class_apply_list=BidApplyList,
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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidQueryAPI[BidList]:
        """Query starting at bids.

        Args:
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bids.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidList)
        return BidQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, bid: BidApply | Sequence[BidApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) bids.

        Args:
            bid: Bid or sequence of bids to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import BidApply
                >>> client = MarketClient()
                >>> bid = BidApply(external_id="my_bid", ...)
                >>> result = client.bid.apply(bid)

        """
        return self._apply(bid, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more bid.

        Args:
            external_id: External id of the bid to delete.
            space: The space where all the bid are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> client.bid.delete("my_bid")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Bid | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> BidList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "market") -> Bid | BidList | None:
        """Retrieve one or more bids by id(s).

        Args:
            external_id: External id or list of external ids of the bids.
            space: The space where all the bids are located.

        Returns:
            The requested bids.

        Examples:

            Retrieve bid by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> bid = client.bid.retrieve("my_bid")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: BidTextFields | Sequence[BidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidList:
        """Search bids

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bids matching the query.

        Examples:

           Search for 'my_bid' in all text properties:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> bids = client.bid.search('my_bid')

        """
        filter_ = _create_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidFields | Sequence[BidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidTextFields | Sequence[BidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: BidFields | Sequence[BidFields] | None = None,
        group_by: BidFields | Sequence[BidFields] = None,
        query: str | None = None,
        search_properties: BidTextFields | Sequence[BidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: BidFields | Sequence[BidFields] | None = None,
        group_by: BidFields | Sequence[BidFields] | None = None,
        query: str | None = None,
        search_property: BidTextFields | Sequence[BidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bids

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bids in space `my_space`:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.bid.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidFields,
        interval: float,
        query: str | None = None,
        search_property: BidTextFields | Sequence[BidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bids

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BID_PROPERTIES_BY_FIELD,
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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidList:
        """List/filter bids

        Args:
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            market: The market to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested bids

        Examples:

            List bids and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> bids = client.bid.list(limit=5)

        """
        filter_ = _create_bid_filter(
            self._view_id,
            min_date,
            max_date,
            market,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
