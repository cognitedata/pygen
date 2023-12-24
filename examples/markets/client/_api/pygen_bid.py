from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from markets.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    PygenBid,
    PygenBidApply,
    PygenBidFields,
    PygenBidList,
    PygenBidApplyList,
    PygenBidTextFields,
)
from markets.client.data_classes._pygen_bid import (
    _PYGENBID_PROPERTIES_BY_FIELD,
    _create_pygen_bid_filter,
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
from .pygen_bid_query import PygenBidQueryAPI


class PygenBidAPI(NodeAPI[PygenBid, PygenBidApply, PygenBidList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PygenBidApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PygenBid,
            class_apply_type=PygenBidApply,
            class_list=PygenBidList,
            class_apply_list=PygenBidApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        is_block: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PygenBidQueryAPI[PygenBidList]:
        """Query starting at pygen bids.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market: The market to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_minimum_price: The minimum value of the minimum price to filter on.
            max_minimum_price: The maximum value of the minimum price to filter on.
            min_price_premium: The minimum value of the price premium to filter on.
            max_price_premium: The maximum value of the price premium to filter on.
            is_block: The is block to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for pygen bids.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_pygen_bid_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            min_date,
            max_date,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            is_block,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PygenBidList)
        return PygenBidQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, pygen_bid: PygenBidApply | Sequence[PygenBidApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) pygen bids.

        Args:
            pygen_bid: Pygen bid or sequence of pygen bids to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new pygen_bid:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import PygenBidApply
                >>> client = MarketClient()
                >>> pygen_bid = PygenBidApply(external_id="my_pygen_bid", ...)
                >>> result = client.pygen_bid.apply(pygen_bid)

        """
        return self._apply(pygen_bid, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
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
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PygenBid | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PygenBidList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PygenBid | PygenBidList | None:
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
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PygenBidTextFields | Sequence[PygenBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        is_block: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenBidList:
        """Search pygen bids

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market: The market to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_minimum_price: The minimum value of the minimum price to filter on.
            max_minimum_price: The maximum value of the minimum price to filter on.
            min_price_premium: The minimum value of the price premium to filter on.
            max_price_premium: The maximum value of the price premium to filter on.
            is_block: The is block to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results pygen bids matching the query.

        Examples:

           Search for 'my_pygen_bid' in all text properties:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_bids = client.pygen_bid.search('my_pygen_bid')

        """
        filter_ = _create_pygen_bid_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            min_date,
            max_date,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            is_block,
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
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        is_block: bool | None = None,
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
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        is_block: bool | None = None,
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
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        is_block: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across pygen bids

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market: The market to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_minimum_price: The minimum value of the minimum price to filter on.
            max_minimum_price: The maximum value of the minimum price to filter on.
            min_price_premium: The minimum value of the price premium to filter on.
            max_price_premium: The maximum value of the price premium to filter on.
            is_block: The is block to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count pygen bids in space `my_space`:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.pygen_bid.aggregate("count", space="my_space")

        """

        filter_ = _create_pygen_bid_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            min_date,
            max_date,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            is_block,
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
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        is_block: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for pygen bids

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market: The market to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_minimum_price: The minimum value of the minimum price to filter on.
            max_minimum_price: The maximum value of the minimum price to filter on.
            min_price_premium: The minimum value of the price premium to filter on.
            max_price_premium: The maximum value of the price premium to filter on.
            is_block: The is block to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_pygen_bid_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            min_date,
            max_date,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            is_block,
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
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_minimum_price: float | None = None,
        max_minimum_price: float | None = None,
        min_price_premium: float | None = None,
        max_price_premium: float | None = None,
        is_block: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenBidList:
        """List/filter pygen bids

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market: The market to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_minimum_price: The minimum value of the minimum price to filter on.
            max_minimum_price: The maximum value of the minimum price to filter on.
            min_price_premium: The minimum value of the price premium to filter on.
            max_price_premium: The maximum value of the price premium to filter on.
            is_block: The is block to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of pygen bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested pygen bids

        Examples:

            List pygen bids and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> pygen_bids = client.pygen_bid.list(limit=5)

        """
        filter_ = _create_pygen_bid_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            min_date,
            max_date,
            min_minimum_price,
            max_minimum_price,
            min_price_premium,
            max_price_premium,
            is_block,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
