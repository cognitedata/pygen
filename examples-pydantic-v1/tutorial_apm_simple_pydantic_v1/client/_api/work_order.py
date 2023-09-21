from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple_pydantic_v1.client.data_classes import WorkOrder, WorkOrderApply, WorkOrderList


class WorkOrderLinkedassetsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.linkedAssets"},
        )
        if isinstance(external_id, str):
            is_work_order = f.Equals(
                ["edge", "startNode"],
                {"space": "tutorial_apm_simple", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_work_order)
            )

        else:
            is_work_orders = f.In(
                ["edge", "startNode"],
                [{"space": "tutorial_apm_simple", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_work_orders)
            )

    def list(self, work_order_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.linkedAssets"},
        )
        filters.append(is_edge_type)
        if work_order_id:
            work_order_ids = [work_order_id] if isinstance(work_order_id, str) else work_order_id
            is_work_orders = f.In(
                ["edge", "startNode"],
                [{"space": "tutorial_apm_simple", "externalId": ext_id} for ext_id in work_order_ids],
            )
            filters.append(is_work_orders)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WorkOrderWorkitemsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.workItems"},
        )
        if isinstance(external_id, str):
            is_work_order = f.Equals(
                ["edge", "startNode"],
                {"space": "tutorial_apm_simple", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_work_order)
            )

        else:
            is_work_orders = f.In(
                ["edge", "startNode"],
                [{"space": "tutorial_apm_simple", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_work_orders)
            )

    def list(self, work_order_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.workItems"},
        )
        filters.append(is_edge_type)
        if work_order_id:
            work_order_ids = [work_order_id] if isinstance(work_order_id, str) else work_order_id
            is_work_orders = f.In(
                ["edge", "startNode"],
                [{"space": "tutorial_apm_simple", "externalId": ext_id} for ext_id in work_order_ids],
            )
            filters.append(is_work_orders)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WorkOrderAPI(TypeAPI[WorkOrder, WorkOrderApply, WorkOrderList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkOrder,
            class_apply_type=WorkOrderApply,
            class_list=WorkOrderList,
        )
        self.view_id = view_id
        self.linked_assets = WorkOrderLinkedassetsAPI(client)
        self.work_items = WorkOrderWorkitemsAPI(client)

    def apply(self, work_order: WorkOrderApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = work_order.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(WorkOrderApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(WorkOrderApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WorkOrder:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WorkOrderList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> WorkOrder | WorkOrderList:
        if isinstance(external_id, str):
            work_order = self._retrieve((self.sources.space, external_id))

            linked_asset_edges = self.linked_assets.retrieve(external_id)
            work_order.linked_assets = [edge.end_node.external_id for edge in linked_asset_edges]
            work_item_edges = self.work_items.retrieve(external_id)
            work_order.work_items = [edge.end_node.external_id for edge in work_item_edges]

            return work_order
        else:
            work_orders = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            linked_asset_edges = self.linked_assets.retrieve(external_id)
            self._set_linked_assets(work_orders, linked_asset_edges)
            work_item_edges = self.work_items.retrieve(external_id)
            self._set_work_items(work_orders, work_item_edges)

            return work_orders

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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WorkOrderList:
        filters = []
        if min_actual_hours or max_actual_hours:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("actualHours"), gte=min_actual_hours, lte=max_actual_hours
                )
            )
        if min_created_date or max_created_date:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("createdDate"), gte=min_created_date, lte=max_created_date
                )
            )
        if description and isinstance(description, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("description"), value=description))
        if description and isinstance(description, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("description"), values=description))
        if description_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("description"), value=description_prefix))
        if min_due_date or max_due_date:
            filters.append(
                dm.filters.Range(self.view_id.as_property_ref("dueDate"), gte=min_due_date, lte=max_due_date)
            )
        if min_duration_hours or max_duration_hours:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("durationHours"), gte=min_duration_hours, lte=max_duration_hours
                )
            )
        if min_end_time or max_end_time:
            filters.append(
                dm.filters.Range(self.view_id.as_property_ref("endTime"), gte=min_end_time, lte=max_end_time)
            )
        if is_active and isinstance(is_active, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("isActive"), value=is_active))
        if is_cancelled and isinstance(is_cancelled, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("isCancelled"), value=is_cancelled))
        if is_completed and isinstance(is_completed, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("isCompleted"), value=is_completed))
        if is_safety_critical and isinstance(is_safety_critical, str):
            filters.append(
                dm.filters.Equals(self.view_id.as_property_ref("isSafetyCritical"), value=is_safety_critical)
            )
        if min_percentage_progress or max_percentage_progress:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("percentageProgress"),
                    gte=min_percentage_progress,
                    lte=max_percentage_progress,
                )
            )
        if min_planned_start or max_planned_start:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("plannedStart"), gte=min_planned_start, lte=max_planned_start
                )
            )
        if priority_description and isinstance(priority_description, str):
            filters.append(
                dm.filters.Equals(self.view_id.as_property_ref("priorityDescription"), value=priority_description)
            )
        if priority_description and isinstance(priority_description, list):
            filters.append(
                dm.filters.In(self.view_id.as_property_ref("priorityDescription"), values=priority_description)
            )
        if priority_description_prefix:
            filters.append(
                dm.filters.Prefix(
                    self.view_id.as_property_ref("priorityDescription"), value=priority_description_prefix
                )
            )
        if program_number and isinstance(program_number, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("programNumber"), value=program_number))
        if program_number and isinstance(program_number, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("programNumber"), values=program_number))
        if program_number_prefix:
            filters.append(
                dm.filters.Prefix(self.view_id.as_property_ref("programNumber"), value=program_number_prefix)
            )
        if min_start_time or max_start_time:
            filters.append(
                dm.filters.Range(self.view_id.as_property_ref("startTime"), gte=min_start_time, lte=max_start_time)
            )
        if status and isinstance(status, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("status"), value=status))
        if status and isinstance(status, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("status"), values=status))
        if status_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("status"), value=status_prefix))
        if title and isinstance(title, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("title"), value=title))
        if title and isinstance(title, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("title"), values=title))
        if title_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("title"), value=title_prefix))
        if work_order_number and isinstance(work_order_number, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("workOrderNumber"), value=work_order_number))
        if work_order_number and isinstance(work_order_number, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("workOrderNumber"), values=work_order_number))
        if work_order_number_prefix:
            filters.append(
                dm.filters.Prefix(self.view_id.as_property_ref("workOrderNumber"), value=work_order_number_prefix)
            )
        if work_package_number and isinstance(work_package_number, str):
            filters.append(
                dm.filters.Equals(self.view_id.as_property_ref("workPackageNumber"), value=work_package_number)
            )
        if work_package_number and isinstance(work_package_number, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("workPackageNumber"), values=work_package_number))
        if work_package_number_prefix:
            filters.append(
                dm.filters.Prefix(self.view_id.as_property_ref("workPackageNumber"), value=work_package_number_prefix)
            )
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        work_orders = self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)

        if retrieve_edges:
            linked_asset_edges = self.linked_assets.list(work_orders.as_external_ids(), limit=-1)
            self._set_linked_assets(work_orders, linked_asset_edges)
            work_item_edges = self.work_items.list(work_orders.as_external_ids(), limit=-1)
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
