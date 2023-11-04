from __future__ import annotations

import datetime
from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets.client.data_classes import Bid, BidApply, BidList, BidApplyList, BidFields, BidTextFields, DomainModelApply
from markets.client.data_classes._bid import _BID_PROPERTIES_BY_FIELD


class BidAPI(TypeAPI[Bid, BidApply, BidList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BidApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Bid,
            class_apply_type=BidApply,
            class_list=BidList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, bid: BidApply | Sequence[BidApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) bids.

        Args:
            bid: Bid or sequence of bids to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new bid:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import BidApply
                >>> client = MarketClient()
                >>> bid = BidApply(external_id="my_bid", ...)
                >>> result = client.bid.apply(bid)

        """
        if isinstance(bid, BidApply):
            instances = bid.to_instances_apply(self._view_by_write_class)
        else:
            instances = BidApplyList(bid).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "market") -> dm.InstancesDeleteResult:
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
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Bid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "market") -> Bid | BidList:
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
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

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
        filter_ = _create_filter(
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

        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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


def _create_filter(
    view_id: dm.ViewId,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_date or max_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("date"),
                gte=min_date.isoformat() if min_date else None,
                lte=max_date.isoformat() if max_date else None,
            )
        )
    if market and isinstance(market, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": "market", "externalId": market})
        )
    if market and isinstance(market, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": market[0], "externalId": market[1]})
        )
    if market and isinstance(market, list) and isinstance(market[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": "market", "externalId": item} for item in market]
            )
        )
    if market and isinstance(market, list) and isinstance(market[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": item[0], "externalId": item[1]} for item in market]
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
