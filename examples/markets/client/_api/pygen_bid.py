from __future__ import annotations

import datetime
from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets.client.data_classes import (
    PygenBid,
    PygenBidApply,
    PygenBidList,
    PygenBidApplyList,
    PygenBidFields,
    PygenBidTextFields,
    DomainModelApply,
)
from markets.client.data_classes._pygen_bid import _PYGENBID_PROPERTIES_BY_FIELD


class PygenBidAPI(TypeAPI[PygenBid, PygenBidApply, PygenBidList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PygenBidApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PygenBid,
            class_apply_type=PygenBidApply,
            class_list=PygenBidList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, pygen_bid: PygenBidApply | Sequence[PygenBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) pygen bids.

        Args:
            pygen_bid: Pygen bid or sequence of pygen bids to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new pygen_bid:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import PygenBidApply
                >>> client = MarketClient()
                >>> pygen_bid = PygenBidApply(external_id="my_pygen_bid", ...)
                >>> result = client.pygen_bid.apply(pygen_bid)

        """
        if isinstance(pygen_bid, PygenBidApply):
            instances = pygen_bid.to_instances_apply(self._view_by_write_class)
        else:
            instances = PygenBidApplyList(pygen_bid).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more pygen bid.

        Args:
            external_id: External id of the pygen bid to delete.
            space: The space where all the pygen bid are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete pygen_bid by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> client.pygen_bid.delete("my_pygen_bid")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PygenBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PygenBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "market") -> PygenBid | PygenBidList:
        """Retrieve one or more pygen bids by id(s).

        Args:
            external_id: External id or list of external ids of the pygen bids.
            space: The space where all the pygen bids are located.

        Returns:
            The requested pygen bids.

        Examples:

            Retrieve pygen_bid by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_bid = client.pygen_bid.retrieve("my_pygen_bid")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: PygenBidTextFields | Sequence[PygenBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        is_block: bool | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenBidList:
        """Search pygen bids

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            is_block: The is block to filter on.
            market: The market to filter on.
            min_minimum_price: The minimum value of the minimum price to filter on.
            max_minimum_price: The maximum value of the minimum price to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_price_premium: The minimum value of the price premium to filter on.
            max_price_premium: The maximum value of the price premium to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results pygen bids matching the query.

        Examples:

           Search for 'my_pygen_bid' in all text properties:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_bids = client.pygen_bid.search('my_pygen_bid')

        """
        filter_ = _create_filter(
            self._view_id,
            min_date,
            max_date,
            is_block,
            market,
            min_minimum_price,
            max_minimum_price,
            name,
            name_prefix,
            min_price_premium,
            max_price_premium,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PYGENBID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PygenBidFields | Sequence[PygenBidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PygenBidTextFields | Sequence[PygenBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        is_block: bool | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
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
        property: PygenBidFields | Sequence[PygenBidFields] | None = None,
        group_by: PygenBidFields | Sequence[PygenBidFields] = None,
        query: str | None = None,
        search_properties: PygenBidTextFields | Sequence[PygenBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        is_block: bool | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
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
        property: PygenBidFields | Sequence[PygenBidFields] | None = None,
        group_by: PygenBidFields | Sequence[PygenBidFields] | None = None,
        query: str | None = None,
        search_property: PygenBidTextFields | Sequence[PygenBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        is_block: bool | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_date,
            max_date,
            is_block,
            market,
            min_minimum_price,
            max_minimum_price,
            name,
            name_prefix,
            min_price_premium,
            max_price_premium,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PYGENBID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PygenBidFields,
        interval: float,
        query: str | None = None,
        search_property: PygenBidTextFields | Sequence[PygenBidTextFields] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        is_block: bool | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_date,
            max_date,
            is_block,
            market,
            min_minimum_price,
            max_minimum_price,
            name,
            name_prefix,
            min_price_premium,
            max_price_premium,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PYGENBID_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        is_block: bool | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenBidList:
        """List/filter pygen bids

        Args:
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            is_block: The is block to filter on.
            market: The market to filter on.
            min_minimum_price: The minimum value of the minimum price to filter on.
            max_minimum_price: The maximum value of the minimum price to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_price_premium: The minimum value of the price premium to filter on.
            max_price_premium: The maximum value of the price premium to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested pygen bids

        Examples:

            List pygen bids and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_bids = client.pygen_bid.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            min_date,
            max_date,
            is_block,
            market,
            min_minimum_price,
            max_minimum_price,
            name,
            name_prefix,
            min_price_premium,
            max_price_premium,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    is_block: bool | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_minimum_price: float | None = None,
    max_minimum_price: float | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_price_premium: float | None = None,
    max_price_premium: float | None = None,
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
    if is_block and isinstance(is_block, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isBlock"), value=is_block))
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
    if min_minimum_price or max_minimum_price:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("minimumPrice"), gte=min_minimum_price, lte=max_minimum_price)
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_price_premium or max_price_premium:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("pricePremium"), gte=min_price_premium, lte=max_price_premium)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
