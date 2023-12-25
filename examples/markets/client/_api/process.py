from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from markets.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Process,
    ProcessApply,
    ProcessFields,
    ProcessList,
    ProcessApplyList,
    ProcessTextFields,
)
from markets.client.data_classes._process import (
    _PROCESS_PROPERTIES_BY_FIELD,
    _create_proces_filter,
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
from .process_query import ProcessQueryAPI


class ProcessAPI(NodeAPI[Process, ProcessApply, ProcessList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ProcessApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Process,
            class_apply_type=ProcessApply,
            class_list=ProcessList,
            class_apply_list=ProcessApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ProcessQueryAPI[ProcessList]:
        """Query starting at process.

        Args:
            bid: The bid to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for process.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_proces_filter(
            self._view_id,
            bid,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ProcessList)
        return ProcessQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, proces: ProcessApply | Sequence[ProcessApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) process.

        Args:
            proces: Proces or sequence of process to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new proces:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import ProcessApply
                >>> client = MarketClient()
                >>> proces = ProcessApply(external_id="my_proces", ...)
                >>> result = client.process.apply(proces)

        """
        return self._apply(proces, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more proces.

        Args:
            external_id: External id of the proces to delete.
            space: The space where all the proces are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete proces by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> client.process.delete("my_proces")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Process | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ProcessList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Process | ProcessList | None:
        """Retrieve one or more process by id(s).

        Args:
            external_id: External id or list of external ids of the process.
            space: The space where all the process are located.

        Returns:
            The requested process.

        Examples:

            Retrieve proces by id:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> proces = client.process.retrieve("my_proces")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ProcessTextFields | Sequence[ProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProcessList:
        """Search process

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            bid: The bid to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results process matching the query.

        Examples:

           Search for 'my_proces' in all text properties:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> process = client.process.search('my_proces')

        """
        filter_ = _create_proces_filter(
            self._view_id,
            bid,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PROCESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ProcessFields | Sequence[ProcessFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ProcessTextFields | Sequence[ProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ProcessFields | Sequence[ProcessFields] | None = None,
        group_by: ProcessFields | Sequence[ProcessFields] = None,
        query: str | None = None,
        search_properties: ProcessTextFields | Sequence[ProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ProcessFields | Sequence[ProcessFields] | None = None,
        group_by: ProcessFields | Sequence[ProcessFields] | None = None,
        query: str | None = None,
        search_property: ProcessTextFields | Sequence[ProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across process

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            bid: The bid to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count process in space `my_space`:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.process.aggregate("count", space="my_space")

        """

        filter_ = _create_proces_filter(
            self._view_id,
            bid,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PROCESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ProcessFields,
        interval: float,
        query: str | None = None,
        search_property: ProcessTextFields | Sequence[ProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for process

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            bid: The bid to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_proces_filter(
            self._view_id,
            bid,
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
            _PROCESS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProcessList:
        """List/filter process

        Args:
            bid: The bid to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of process to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested process

        Examples:

            List process and limit to 5:

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> process = client.process.list(limit=5)

        """
        filter_ = _create_proces_filter(
            self._view_id,
            bid,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
