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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.linkedAssets"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkOrder.workItems"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class WorkOrderAPI(TypeAPI[WorkOrder, WorkOrderApply, WorkOrderList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkOrder,
            class_apply_type=WorkOrderApply,
            class_list=WorkOrderList,
        )
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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> WorkOrderList:
        work_orders = self._list(limit=limit)

        linked_asset_edges = self.linked_assets.list(limit=-1)
        self._set_linked_assets(work_orders, linked_asset_edges)
        work_item_edges = self.work_items.list(limit=-1)
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
