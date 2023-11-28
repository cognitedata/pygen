from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CogProcess,
    CogProcessApply,
    CogProcessFields,
    CogProcessList,
    CogProcessApplyList,
    CogProcessTextFields,
)
from markets.client.data_classes._cog_process import (
    _COGPROCESS_PROPERTIES_BY_FIELD,
    _create_cog_proces_filter,
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
from .cog_process_query import CogProcessQueryAPI


class CogProcessAPI(NodeAPI[CogProcess, CogProcessApply, CogProcessList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CogProcessApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogProcess,
            class_apply_type=CogProcessApply,
            class_list=CogProcessList,
            class_apply_list=CogProcessApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogProcessQueryAPI[CogProcessList]:
        """Query starting at cog process.

        Args:
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cog process.

        """
        filter_ = _create_cog_proces_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            CogProcessList,
            [
                QueryStep(
                    name="cog_proces",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_COGPROCESS_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=CogProcess,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return CogProcessQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, cog_proces: CogProcessApply | Sequence[CogProcessApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) cog process.

        Args:
            cog_proces: Cog proces or sequence of cog process to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cog_proces:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import CogProcessApply
                >>> client = MarketClient()
                >>> cog_proces = CogProcessApply(external_id="my_cog_proces", ...)
                >>> result = client.cog_process.apply(cog_proces)

        """
        return self._apply(cog_proces, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = "market") -> dm.InstancesDeleteResult:
        """Delete one or more cog proces.

        Args:
            external_id: External id of the cog proces to delete.
            space: The space where all the cog proces are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cog_proces by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> client.cog_process.delete("my_cog_proces")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> CogProcess | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> CogProcessList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "market"
    ) -> CogProcess | CogProcessList | None:
        """Retrieve one or more cog process by id(s).

        Args:
            external_id: External id or list of external ids of the cog process.
            space: The space where all the cog process are located.

        Returns:
            The requested cog process.

        Examples:

            Retrieve cog_proces by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> cog_proces = client.cog_process.retrieve("my_cog_proces")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogProcessList:
        """Search cog process

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results cog process matching the query.

        Examples:

           Search for 'my_cog_proces' in all text properties:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> cog_process = client.cog_process.search('my_cog_proces')

        """
        filter_ = _create_cog_proces_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _COGPROCESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: CogProcessFields | Sequence[CogProcessFields] = None,
        query: str | None = None,
        search_properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: CogProcessFields | Sequence[CogProcessFields] | None = None,
        query: str | None = None,
        search_property: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cog process

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cog process in space `my_space`:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.cog_process.aggregate("count", space="my_space")

        """

        filter_ = _create_cog_proces_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _COGPROCESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CogProcessFields,
        interval: float,
        query: str | None = None,
        search_property: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cog process

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cog_proces_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _COGPROCESS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogProcessList:
        """List/filter cog process

        Args:
            bid: The bid to filter on.
            date_transformations: The date transformation to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            transformation: The transformation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cog process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cog process

        Examples:

            List cog process and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> cog_process = client.cog_process.list(limit=5)

        """
        filter_ = _create_cog_proces_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
