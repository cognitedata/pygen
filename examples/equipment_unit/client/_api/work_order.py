from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from equipment_unit.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from equipment_unit.client.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    WorkOrder,
    WorkOrderApply,
    WorkOrderFields,
    WorkOrderList,
    WorkOrderApplyList,
    WorkOrderTextFields,
)
from equipment_unit.client.data_classes._work_order import (
    _WORKORDER_PROPERTIES_BY_FIELD,
    _create_work_order_filter,
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
from .work_order_query import WorkOrderQueryAPI


class WorkOrderAPI(NodeAPI[WorkOrder, WorkOrderApply, WorkOrderList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[WorkOrder]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkOrder,
            class_list=WorkOrderList,
            class_apply_list=WorkOrderApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WorkOrderQueryAPI[WorkOrderList]:
        """Query starting at work orders.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            performed_by: The performed by to filter on.
            performed_by_prefix: The prefix of the performed by to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for work orders.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_work_order_filter(
            self._view_id,
            description,
            description_prefix,
            performed_by,
            performed_by_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WorkOrderList)
        return WorkOrderQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        work_order: WorkOrderApply | Sequence[WorkOrderApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) work orders.

        Args:
            work_order: Work order or sequence of work orders to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): Should we write None values to the API? If False, None values will be ignored. If True, None values will be written to the API.
                Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new work_order:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> from equipment_unit.client.data_classes import WorkOrderApply
                >>> client = EquipmentUnitClient()
                >>> work_order = WorkOrderApply(external_id="my_work_order", ...)
                >>> result = client.work_order.apply(work_order)

        """
        return self._apply(work_order, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more work order.

        Args:
            external_id: External id of the work order to delete.
            space: The space where all the work order are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete work_order by id:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> client.work_order.delete("my_work_order")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> WorkOrder | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> WorkOrderList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> WorkOrder | WorkOrderList | None:
        """Retrieve one or more work orders by id(s).

        Args:
            external_id: External id or list of external ids of the work orders.
            space: The space where all the work orders are located.

        Returns:
            The requested work orders.

        Examples:

            Retrieve work_order by id:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> work_order = client.work_order.retrieve("my_work_order")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: WorkOrderTextFields | Sequence[WorkOrderTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WorkOrderList:
        """Search work orders

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            performed_by: The performed by to filter on.
            performed_by_prefix: The prefix of the performed by to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results work orders matching the query.

        Examples:

           Search for 'my_work_order' in all text properties:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> work_orders = client.work_order.search('my_work_order')

        """
        filter_ = _create_work_order_filter(
            self._view_id,
            description,
            description_prefix,
            performed_by,
            performed_by_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WORKORDER_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WorkOrderFields | Sequence[WorkOrderFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WorkOrderTextFields | Sequence[WorkOrderTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: WorkOrderFields | Sequence[WorkOrderFields] | None = None,
        group_by: WorkOrderFields | Sequence[WorkOrderFields] = None,
        query: str | None = None,
        search_properties: WorkOrderTextFields | Sequence[WorkOrderTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: WorkOrderFields | Sequence[WorkOrderFields] | None = None,
        group_by: WorkOrderFields | Sequence[WorkOrderFields] | None = None,
        query: str | None = None,
        search_property: WorkOrderTextFields | Sequence[WorkOrderTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across work orders

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            performed_by: The performed by to filter on.
            performed_by_prefix: The prefix of the performed by to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count work orders in space `my_space`:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> result = client.work_order.aggregate("count", space="my_space")

        """

        filter_ = _create_work_order_filter(
            self._view_id,
            description,
            description_prefix,
            performed_by,
            performed_by_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WORKORDER_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WorkOrderFields,
        interval: float,
        query: str | None = None,
        search_property: WorkOrderTextFields | Sequence[WorkOrderTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for work orders

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            performed_by: The performed by to filter on.
            performed_by_prefix: The prefix of the performed by to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_work_order_filter(
            self._view_id,
            description,
            description_prefix,
            performed_by,
            performed_by_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WORKORDER_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WorkOrderList:
        """List/filter work orders

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            performed_by: The performed by to filter on.
            performed_by_prefix: The prefix of the performed by to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested work orders

        Examples:

            List work orders and limit to 5:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> work_orders = client.work_order.list(limit=5)

        """
        filter_ = _create_work_order_filter(
            self._view_id,
            description,
            description_prefix,
            performed_by,
            performed_by_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
