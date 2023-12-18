from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from tutorial_apm_simple_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WorkOrder,
    WorkOrderApply,
    WorkOrderFields,
    WorkOrderList,
    WorkOrderApplyList,
    WorkOrderTextFields,
)
from tutorial_apm_simple_pydantic_v1.client.data_classes._work_order import (
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
from .work_order_linked_assets import WorkOrderLinkedAssetsAPI
from .work_order_work_items import WorkOrderWorkItemsAPI
from .work_order_query import WorkOrderQueryAPI


class WorkOrderAPI(NodeAPI[WorkOrder, WorkOrderApply, WorkOrderList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WorkOrderApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkOrder,
            class_apply_type=WorkOrderApply,
            class_list=WorkOrderList,
            class_apply_list=WorkOrderApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.linked_assets_edge = WorkOrderLinkedAssetsAPI(client)
        self.work_items_edge = WorkOrderWorkItemsAPI(client)

    def __call__(
        self,
        min_actual_hours: int | None = None,
        max_actual_hours: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_due_date: datetime.datetime | None = None,
        max_due_date: datetime.datetime | None = None,
        min_duration_hours: int | None = None,
        max_duration_hours: int | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        is_active: bool | None = None,
        is_cancelled: bool | None = None,
        is_completed: bool | None = None,
        is_safety_critical: bool | None = None,
        min_percentage_progress: int | None = None,
        max_percentage_progress: int | None = None,
        min_planned_start: datetime.datetime | None = None,
        max_planned_start: datetime.datetime | None = None,
        priority_description: str | list[str] | None = None,
        priority_description_prefix: str | None = None,
        program_number: str | list[str] | None = None,
        program_number_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        work_order_number: str | list[str] | None = None,
        work_order_number_prefix: str | None = None,
        work_package_number: str | list[str] | None = None,
        work_package_number_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WorkOrderQueryAPI[WorkOrderList]:
        """Query starting at work orders.

        Args:
            min_actual_hours: The minimum value of the actual hour to filter on.
            max_actual_hours: The maximum value of the actual hour to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            min_due_date: The minimum value of the due date to filter on.
            max_due_date: The maximum value of the due date to filter on.
            min_duration_hours: The minimum value of the duration hour to filter on.
            max_duration_hours: The maximum value of the duration hour to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            is_active: The is active to filter on.
            is_cancelled: The is cancelled to filter on.
            is_completed: The is completed to filter on.
            is_safety_critical: The is safety critical to filter on.
            min_percentage_progress: The minimum value of the percentage progres to filter on.
            max_percentage_progress: The maximum value of the percentage progres to filter on.
            min_planned_start: The minimum value of the planned start to filter on.
            max_planned_start: The maximum value of the planned start to filter on.
            priority_description: The priority description to filter on.
            priority_description_prefix: The prefix of the priority description to filter on.
            program_number: The program number to filter on.
            program_number_prefix: The prefix of the program number to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            work_order_number: The work order number to filter on.
            work_order_number_prefix: The prefix of the work order number to filter on.
            work_package_number: The work package number to filter on.
            work_package_number_prefix: The prefix of the work package number to filter on.
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
            min_actual_hours,
            max_actual_hours,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            min_due_date,
            max_due_date,
            min_duration_hours,
            max_duration_hours,
            min_end_time,
            max_end_time,
            is_active,
            is_cancelled,
            is_completed,
            is_safety_critical,
            min_percentage_progress,
            max_percentage_progress,
            min_planned_start,
            max_planned_start,
            priority_description,
            priority_description_prefix,
            program_number,
            program_number_prefix,
            min_start_time,
            max_start_time,
            status,
            status_prefix,
            title,
            title_prefix,
            work_order_number,
            work_order_number_prefix,
            work_package_number,
            work_package_number_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WorkOrderList)
        return WorkOrderQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, work_order: WorkOrderApply | Sequence[WorkOrderApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) work orders.

        Note: This method iterates through all nodes and timeseries linked to work_order and creates them including the edges
        between the nodes. For example, if any of `linked_assets` or `work_items` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            work_order: Work order or sequence of work orders to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new work_order:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> from tutorial_apm_simple_pydantic_v1.client.data_classes import WorkOrderApply
                >>> client = ApmSimpleClient()
                >>> work_order = WorkOrderApply(external_id="my_work_order", ...)
                >>> result = client.work_order.apply(work_order)

        """
        return self._apply(work_order, replace)

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

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
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

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_order = client.work_order.retrieve("my_work_order")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.linked_assets_edge,
                    "linked_assets",
                    dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.linkedAssets"),
                    "outwards",
                ),
                (
                    self.work_items_edge,
                    "work_items",
                    dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.workItems"),
                    "outwards",
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WorkOrderTextFields | Sequence[WorkOrderTextFields] | None = None,
        min_actual_hours: int | None = None,
        max_actual_hours: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_due_date: datetime.datetime | None = None,
        max_due_date: datetime.datetime | None = None,
        min_duration_hours: int | None = None,
        max_duration_hours: int | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        is_active: bool | None = None,
        is_cancelled: bool | None = None,
        is_completed: bool | None = None,
        is_safety_critical: bool | None = None,
        min_percentage_progress: int | None = None,
        max_percentage_progress: int | None = None,
        min_planned_start: datetime.datetime | None = None,
        max_planned_start: datetime.datetime | None = None,
        priority_description: str | list[str] | None = None,
        priority_description_prefix: str | None = None,
        program_number: str | list[str] | None = None,
        program_number_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        work_order_number: str | list[str] | None = None,
        work_order_number_prefix: str | None = None,
        work_package_number: str | list[str] | None = None,
        work_package_number_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WorkOrderList:
        """Search work orders

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_actual_hours: The minimum value of the actual hour to filter on.
            max_actual_hours: The maximum value of the actual hour to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            min_due_date: The minimum value of the due date to filter on.
            max_due_date: The maximum value of the due date to filter on.
            min_duration_hours: The minimum value of the duration hour to filter on.
            max_duration_hours: The maximum value of the duration hour to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            is_active: The is active to filter on.
            is_cancelled: The is cancelled to filter on.
            is_completed: The is completed to filter on.
            is_safety_critical: The is safety critical to filter on.
            min_percentage_progress: The minimum value of the percentage progres to filter on.
            max_percentage_progress: The maximum value of the percentage progres to filter on.
            min_planned_start: The minimum value of the planned start to filter on.
            max_planned_start: The maximum value of the planned start to filter on.
            priority_description: The priority description to filter on.
            priority_description_prefix: The prefix of the priority description to filter on.
            program_number: The program number to filter on.
            program_number_prefix: The prefix of the program number to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            work_order_number: The work order number to filter on.
            work_order_number_prefix: The prefix of the work order number to filter on.
            work_package_number: The work package number to filter on.
            work_package_number_prefix: The prefix of the work package number to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results work orders matching the query.

        Examples:

           Search for 'my_work_order' in all text properties:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_orders = client.work_order.search('my_work_order')

        """
        filter_ = _create_work_order_filter(
            self._view_id,
            min_actual_hours,
            max_actual_hours,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            min_due_date,
            max_due_date,
            min_duration_hours,
            max_duration_hours,
            min_end_time,
            max_end_time,
            is_active,
            is_cancelled,
            is_completed,
            is_safety_critical,
            min_percentage_progress,
            max_percentage_progress,
            min_planned_start,
            max_planned_start,
            priority_description,
            priority_description_prefix,
            program_number,
            program_number_prefix,
            min_start_time,
            max_start_time,
            status,
            status_prefix,
            title,
            title_prefix,
            work_order_number,
            work_order_number_prefix,
            work_package_number,
            work_package_number_prefix,
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
        min_actual_hours: int | None = None,
        max_actual_hours: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_due_date: datetime.datetime | None = None,
        max_due_date: datetime.datetime | None = None,
        min_duration_hours: int | None = None,
        max_duration_hours: int | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        is_active: bool | None = None,
        is_cancelled: bool | None = None,
        is_completed: bool | None = None,
        is_safety_critical: bool | None = None,
        min_percentage_progress: int | None = None,
        max_percentage_progress: int | None = None,
        min_planned_start: datetime.datetime | None = None,
        max_planned_start: datetime.datetime | None = None,
        priority_description: str | list[str] | None = None,
        priority_description_prefix: str | None = None,
        program_number: str | list[str] | None = None,
        program_number_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        work_order_number: str | list[str] | None = None,
        work_order_number_prefix: str | None = None,
        work_package_number: str | list[str] | None = None,
        work_package_number_prefix: str | None = None,
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
        min_actual_hours: int | None = None,
        max_actual_hours: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_due_date: datetime.datetime | None = None,
        max_due_date: datetime.datetime | None = None,
        min_duration_hours: int | None = None,
        max_duration_hours: int | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        is_active: bool | None = None,
        is_cancelled: bool | None = None,
        is_completed: bool | None = None,
        is_safety_critical: bool | None = None,
        min_percentage_progress: int | None = None,
        max_percentage_progress: int | None = None,
        min_planned_start: datetime.datetime | None = None,
        max_planned_start: datetime.datetime | None = None,
        priority_description: str | list[str] | None = None,
        priority_description_prefix: str | None = None,
        program_number: str | list[str] | None = None,
        program_number_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        work_order_number: str | list[str] | None = None,
        work_order_number_prefix: str | None = None,
        work_package_number: str | list[str] | None = None,
        work_package_number_prefix: str | None = None,
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
        min_actual_hours: int | None = None,
        max_actual_hours: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_due_date: datetime.datetime | None = None,
        max_due_date: datetime.datetime | None = None,
        min_duration_hours: int | None = None,
        max_duration_hours: int | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        is_active: bool | None = None,
        is_cancelled: bool | None = None,
        is_completed: bool | None = None,
        is_safety_critical: bool | None = None,
        min_percentage_progress: int | None = None,
        max_percentage_progress: int | None = None,
        min_planned_start: datetime.datetime | None = None,
        max_planned_start: datetime.datetime | None = None,
        priority_description: str | list[str] | None = None,
        priority_description_prefix: str | None = None,
        program_number: str | list[str] | None = None,
        program_number_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        work_order_number: str | list[str] | None = None,
        work_order_number_prefix: str | None = None,
        work_package_number: str | list[str] | None = None,
        work_package_number_prefix: str | None = None,
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
            min_actual_hours: The minimum value of the actual hour to filter on.
            max_actual_hours: The maximum value of the actual hour to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            min_due_date: The minimum value of the due date to filter on.
            max_due_date: The maximum value of the due date to filter on.
            min_duration_hours: The minimum value of the duration hour to filter on.
            max_duration_hours: The maximum value of the duration hour to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            is_active: The is active to filter on.
            is_cancelled: The is cancelled to filter on.
            is_completed: The is completed to filter on.
            is_safety_critical: The is safety critical to filter on.
            min_percentage_progress: The minimum value of the percentage progres to filter on.
            max_percentage_progress: The maximum value of the percentage progres to filter on.
            min_planned_start: The minimum value of the planned start to filter on.
            max_planned_start: The maximum value of the planned start to filter on.
            priority_description: The priority description to filter on.
            priority_description_prefix: The prefix of the priority description to filter on.
            program_number: The program number to filter on.
            program_number_prefix: The prefix of the program number to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            work_order_number: The work order number to filter on.
            work_order_number_prefix: The prefix of the work order number to filter on.
            work_package_number: The work package number to filter on.
            work_package_number_prefix: The prefix of the work package number to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count work orders in space `my_space`:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.work_order.aggregate("count", space="my_space")

        """

        filter_ = _create_work_order_filter(
            self._view_id,
            min_actual_hours,
            max_actual_hours,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            min_due_date,
            max_due_date,
            min_duration_hours,
            max_duration_hours,
            min_end_time,
            max_end_time,
            is_active,
            is_cancelled,
            is_completed,
            is_safety_critical,
            min_percentage_progress,
            max_percentage_progress,
            min_planned_start,
            max_planned_start,
            priority_description,
            priority_description_prefix,
            program_number,
            program_number_prefix,
            min_start_time,
            max_start_time,
            status,
            status_prefix,
            title,
            title_prefix,
            work_order_number,
            work_order_number_prefix,
            work_package_number,
            work_package_number_prefix,
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
        min_actual_hours: int | None = None,
        max_actual_hours: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_due_date: datetime.datetime | None = None,
        max_due_date: datetime.datetime | None = None,
        min_duration_hours: int | None = None,
        max_duration_hours: int | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        is_active: bool | None = None,
        is_cancelled: bool | None = None,
        is_completed: bool | None = None,
        is_safety_critical: bool | None = None,
        min_percentage_progress: int | None = None,
        max_percentage_progress: int | None = None,
        min_planned_start: datetime.datetime | None = None,
        max_planned_start: datetime.datetime | None = None,
        priority_description: str | list[str] | None = None,
        priority_description_prefix: str | None = None,
        program_number: str | list[str] | None = None,
        program_number_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        work_order_number: str | list[str] | None = None,
        work_order_number_prefix: str | None = None,
        work_package_number: str | list[str] | None = None,
        work_package_number_prefix: str | None = None,
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
            min_actual_hours: The minimum value of the actual hour to filter on.
            max_actual_hours: The maximum value of the actual hour to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            min_due_date: The minimum value of the due date to filter on.
            max_due_date: The maximum value of the due date to filter on.
            min_duration_hours: The minimum value of the duration hour to filter on.
            max_duration_hours: The maximum value of the duration hour to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            is_active: The is active to filter on.
            is_cancelled: The is cancelled to filter on.
            is_completed: The is completed to filter on.
            is_safety_critical: The is safety critical to filter on.
            min_percentage_progress: The minimum value of the percentage progres to filter on.
            max_percentage_progress: The maximum value of the percentage progres to filter on.
            min_planned_start: The minimum value of the planned start to filter on.
            max_planned_start: The maximum value of the planned start to filter on.
            priority_description: The priority description to filter on.
            priority_description_prefix: The prefix of the priority description to filter on.
            program_number: The program number to filter on.
            program_number_prefix: The prefix of the program number to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            work_order_number: The work order number to filter on.
            work_order_number_prefix: The prefix of the work order number to filter on.
            work_package_number: The work package number to filter on.
            work_package_number_prefix: The prefix of the work package number to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_work_order_filter(
            self._view_id,
            min_actual_hours,
            max_actual_hours,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            min_due_date,
            max_due_date,
            min_duration_hours,
            max_duration_hours,
            min_end_time,
            max_end_time,
            is_active,
            is_cancelled,
            is_completed,
            is_safety_critical,
            min_percentage_progress,
            max_percentage_progress,
            min_planned_start,
            max_planned_start,
            priority_description,
            priority_description_prefix,
            program_number,
            program_number_prefix,
            min_start_time,
            max_start_time,
            status,
            status_prefix,
            title,
            title_prefix,
            work_order_number,
            work_order_number_prefix,
            work_package_number,
            work_package_number_prefix,
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
        min_actual_hours: int | None = None,
        max_actual_hours: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        min_due_date: datetime.datetime | None = None,
        max_due_date: datetime.datetime | None = None,
        min_duration_hours: int | None = None,
        max_duration_hours: int | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        is_active: bool | None = None,
        is_cancelled: bool | None = None,
        is_completed: bool | None = None,
        is_safety_critical: bool | None = None,
        min_percentage_progress: int | None = None,
        max_percentage_progress: int | None = None,
        min_planned_start: datetime.datetime | None = None,
        max_planned_start: datetime.datetime | None = None,
        priority_description: str | list[str] | None = None,
        priority_description_prefix: str | None = None,
        program_number: str | list[str] | None = None,
        program_number_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        work_order_number: str | list[str] | None = None,
        work_order_number_prefix: str | None = None,
        work_package_number: str | list[str] | None = None,
        work_package_number_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WorkOrderList:
        """List/filter work orders

        Args:
            min_actual_hours: The minimum value of the actual hour to filter on.
            max_actual_hours: The maximum value of the actual hour to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            min_due_date: The minimum value of the due date to filter on.
            max_due_date: The maximum value of the due date to filter on.
            min_duration_hours: The minimum value of the duration hour to filter on.
            max_duration_hours: The maximum value of the duration hour to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            is_active: The is active to filter on.
            is_cancelled: The is cancelled to filter on.
            is_completed: The is completed to filter on.
            is_safety_critical: The is safety critical to filter on.
            min_percentage_progress: The minimum value of the percentage progres to filter on.
            max_percentage_progress: The maximum value of the percentage progres to filter on.
            min_planned_start: The minimum value of the planned start to filter on.
            max_planned_start: The maximum value of the planned start to filter on.
            priority_description: The priority description to filter on.
            priority_description_prefix: The prefix of the priority description to filter on.
            program_number: The program number to filter on.
            program_number_prefix: The prefix of the program number to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            status: The status to filter on.
            status_prefix: The prefix of the status to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            work_order_number: The work order number to filter on.
            work_order_number_prefix: The prefix of the work order number to filter on.
            work_package_number: The work package number to filter on.
            work_package_number_prefix: The prefix of the work package number to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work orders to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `linked_assets` or `work_items` external ids for the work orders. Defaults to True.

        Returns:
            List of requested work orders

        Examples:

            List work orders and limit to 5:

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_orders = client.work_order.list(limit=5)

        """
        filter_ = _create_work_order_filter(
            self._view_id,
            min_actual_hours,
            max_actual_hours,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            min_due_date,
            max_due_date,
            min_duration_hours,
            max_duration_hours,
            min_end_time,
            max_end_time,
            is_active,
            is_cancelled,
            is_completed,
            is_safety_critical,
            min_percentage_progress,
            max_percentage_progress,
            min_planned_start,
            max_planned_start,
            priority_description,
            priority_description_prefix,
            program_number,
            program_number_prefix,
            min_start_time,
            max_start_time,
            status,
            status_prefix,
            title,
            title_prefix,
            work_order_number,
            work_order_number_prefix,
            work_package_number,
            work_package_number_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_quad=[
                (
                    self.linked_assets_edge,
                    "linked_assets",
                    dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.linkedAssets"),
                    "outwards",
                ),
                (
                    self.work_items_edge,
                    "work_items",
                    dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.workItems"),
                    "outwards",
                ),
            ],
        )
