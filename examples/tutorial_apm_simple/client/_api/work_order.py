from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from tutorial_apm_simple.client.data_classes import (
    WorkOrder,
    WorkOrderApply,
    WorkOrderList,
    WorkOrderApplyList,
    WorkOrderFields,
    WorkOrderTextFields,
    DomainModelApply,
)
from tutorial_apm_simple.client.data_classes._work_order import _WORKORDER_PROPERTIES_BY_FIELD


class WorkOrderLinkedAssetsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "tutorial_apm_simple"
    ) -> dm.EdgeList:
        """Retrieve one or more linked_assets edges by id(s) of a work order.

        Args:
            external_id: External id or list of external ids source work order.
            space: The space where all the linked asset edges are located.

        Returns:
            The requested linked asset edges.

        Examples:

            Retrieve linked_assets edge by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_order = client.work_order.linked_assets.retrieve("my_linked_assets")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.linkedAssets"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_work_orders = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_work_orders = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_work_orders))

    def list(
        self,
        work_order_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "tutorial_apm_simple",
    ) -> dm.EdgeList:
        """List linked_assets edges of a work order.

        Args:
            work_order_id: ID of the source work order.
            limit: Maximum number of linked asset edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the linked asset edges are located.

        Returns:
            The requested linked asset edges.

        Examples:

            List 5 linked_assets edges connected to "my_work_order":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_order = client.work_order.linked_assets.list("my_work_order", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "tutorial_apm_simple", "externalId": "WorkOrder.linkedAssets"},
            )
        ]
        if work_order_id:
            work_order_ids = work_order_id if isinstance(work_order_id, list) else [work_order_id]
            is_work_orders = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in work_order_ids
                ],
            )
            filters.append(is_work_orders)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WorkOrderWorkItemsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "tutorial_apm_simple"
    ) -> dm.EdgeList:
        """Retrieve one or more work_items edges by id(s) of a work order.

        Args:
            external_id: External id or list of external ids source work order.
            space: The space where all the work item edges are located.

        Returns:
            The requested work item edges.

        Examples:

            Retrieve work_items edge by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_order = client.work_order.work_items.retrieve("my_work_items")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.workItems"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_work_orders = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_work_orders = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_work_orders))

    def list(
        self,
        work_order_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "tutorial_apm_simple",
    ) -> dm.EdgeList:
        """List work_items edges of a work order.

        Args:
            work_order_id: ID of the source work order.
            limit: Maximum number of work item edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the work item edges are located.

        Returns:
            The requested work item edges.

        Examples:

            List 5 work_items edges connected to "my_work_order":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_order = client.work_order.work_items.list("my_work_order", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "tutorial_apm_simple", "externalId": "WorkOrder.workItems"},
            )
        ]
        if work_order_id:
            work_order_ids = work_order_id if isinstance(work_order_id, list) else [work_order_id]
            is_work_orders = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in work_order_ids
                ],
            )
            filters.append(is_work_orders)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WorkOrderAPI(TypeAPI[WorkOrder, WorkOrderApply, WorkOrderList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WorkOrderApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkOrder,
            class_apply_type=WorkOrderApply,
            class_list=WorkOrderList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.linked_assets = WorkOrderLinkedAssetsAPI(client)
        self.work_items = WorkOrderWorkItemsAPI(client)

    def apply(
        self, work_order: WorkOrderApply | Sequence[WorkOrderApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) work orders.

        Note: This method iterates through all nodes linked to work_order and create them including the edges
        between the nodes. For example, if any of `linked_assets` or `work_items` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            work_order: Work order or sequence of work orders to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new work_order:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import WorkOrderApply
                >>> client = ApmSimpleClient()
                >>> work_order = WorkOrderApply(external_id="my_work_order", ...)
                >>> result = client.work_order.apply(work_order)

        """
        if isinstance(work_order, WorkOrderApply):
            instances = work_order.to_instances_apply(self._view_by_write_class)
        else:
            instances = WorkOrderApplyList(work_order).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple") -> dm.InstancesDeleteResult:
        """Delete one or more work order.

        Args:
            external_id: External id of the work order to delete.
            space: The space where all the work order are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete work_order by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.work_order.delete("my_work_order")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WorkOrder:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WorkOrderList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple"
    ) -> WorkOrder | WorkOrderList:
        """Retrieve one or more work orders by id(s).

        Args:
            external_id: External id or list of external ids of the work orders.
            space: The space where all the work orders are located.

        Returns:
            The requested work orders.

        Examples:

            Retrieve work_order by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_order = client.work_order.retrieve("my_work_order")

        """
        if isinstance(external_id, str):
            work_order = self._retrieve((space, external_id))

            linked_asset_edges = self.linked_assets.retrieve(external_id, space=space)
            work_order.linked_assets = [edge.end_node.external_id for edge in linked_asset_edges]
            work_item_edges = self.work_items.retrieve(external_id, space=space)
            work_order.work_items = [edge.end_node.external_id for edge in work_item_edges]

            return work_order
        else:
            work_orders = self._retrieve([(space, ext_id) for ext_id in external_id])

            linked_asset_edges = self.linked_assets.retrieve(work_orders.as_node_ids())
            self._set_linked_assets(work_orders, linked_asset_edges)
            work_item_edges = self.work_items.retrieve(work_orders.as_node_ids())
            self._set_work_items(work_orders, work_item_edges)

            return work_orders

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

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_orders = client.work_order.search('my_work_order')

        """
        filter_ = _create_filter(
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

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.work_order.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
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
            retrieve_edges: Whether to retrieve `linked_assets` or `work_items` external ids for the work orders. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
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

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_orders = client.work_order.list(limit=5)

        """
        filter_ = _create_filter(
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

        work_orders = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := work_orders.as_node_ids()) > IN_FILTER_LIMIT:
                linked_asset_edges = self.linked_assets.list(limit=-1, **space_arg)
            else:
                linked_asset_edges = self.linked_assets.list(ids, limit=-1)
            self._set_linked_assets(work_orders, linked_asset_edges)
            if len(ids := work_orders.as_node_ids()) > IN_FILTER_LIMIT:
                work_item_edges = self.work_items.list(limit=-1, **space_arg)
            else:
                work_item_edges = self.work_items.list(ids, limit=-1)
            self._set_work_items(work_orders, work_item_edges)

        return work_orders

    @staticmethod
    def _set_linked_assets(work_orders: Sequence[WorkOrder], linked_asset_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in linked_asset_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for work_order in work_orders:
            node_id = work_order.id_tuple()
            if node_id in edges_by_start_node:
                work_order.linked_assets = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_work_items(work_orders: Sequence[WorkOrder], work_item_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in work_item_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for work_order in work_orders:
            node_id = work_order.id_tuple()
            if node_id in edges_by_start_node:
                work_order.work_items = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
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
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_actual_hours or max_actual_hours:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("actualHours"), gte=min_actual_hours, lte=max_actual_hours)
        )
    if min_created_date or max_created_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("createdDate"),
                gte=min_created_date.isoformat(timespec="milliseconds") if min_created_date else None,
                lte=max_created_date.isoformat(timespec="milliseconds") if max_created_date else None,
            )
        )
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if min_due_date or max_due_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("dueDate"),
                gte=min_due_date.isoformat(timespec="milliseconds") if min_due_date else None,
                lte=max_due_date.isoformat(timespec="milliseconds") if max_due_date else None,
            )
        )
    if min_duration_hours or max_duration_hours:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("durationHours"), gte=min_duration_hours, lte=max_duration_hours)
        )
    if min_end_time or max_end_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endTime"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if is_active and isinstance(is_active, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isActive"), value=is_active))
    if is_cancelled and isinstance(is_cancelled, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCancelled"), value=is_cancelled))
    if is_completed and isinstance(is_completed, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCompleted"), value=is_completed))
    if is_safety_critical and isinstance(is_safety_critical, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isSafetyCritical"), value=is_safety_critical))
    if min_percentage_progress or max_percentage_progress:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("percentageProgress"), gte=min_percentage_progress, lte=max_percentage_progress
            )
        )
    if min_planned_start or max_planned_start:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("plannedStart"),
                gte=min_planned_start.isoformat(timespec="milliseconds") if min_planned_start else None,
                lte=max_planned_start.isoformat(timespec="milliseconds") if max_planned_start else None,
            )
        )
    if priority_description and isinstance(priority_description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priorityDescription"), value=priority_description))
    if priority_description and isinstance(priority_description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priorityDescription"), values=priority_description))
    if priority_description_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("priorityDescription"), value=priority_description_prefix)
        )
    if program_number and isinstance(program_number, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("programNumber"), value=program_number))
    if program_number and isinstance(program_number, list):
        filters.append(dm.filters.In(view_id.as_property_ref("programNumber"), values=program_number))
    if program_number_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("programNumber"), value=program_number_prefix))
    if min_start_time or max_start_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startTime"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if status and isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
    if status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("status"), value=status_prefix))
    if title and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if work_order_number and isinstance(work_order_number, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workOrderNumber"), value=work_order_number))
    if work_order_number and isinstance(work_order_number, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workOrderNumber"), values=work_order_number))
    if work_order_number_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("workOrderNumber"), value=work_order_number_prefix))
    if work_package_number and isinstance(work_package_number, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workPackageNumber"), value=work_package_number))
    if work_package_number and isinstance(work_package_number, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workPackageNumber"), values=work_package_number))
    if work_package_number_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("workPackageNumber"), value=work_package_number_prefix)
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
