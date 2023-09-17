from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple.client.data_classes import WorkItem, WorkItemApply, WorkItemList


class WorkItemLinkedassetsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkItem.linkedAssets"},
        )
        if isinstance(external_id, str):
            is_work_item = f.Equals(
                ["edge", "startNode"],
                {"space": "tutorial_apm_simple", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_work_item))

        else:
            is_work_items = f.In(
                ["edge", "startNode"],
                [{"space": "tutorial_apm_simple", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_work_items)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkItem.linkedAssets"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class WorksAPI(TypeAPI[WorkItem, WorkItemApply, WorkItemList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("tutorial_apm_simple", "WorkItem", "18ac48abbe96aa"),
            class_type=WorkItem,
            class_apply_type=WorkItemApply,
            class_list=WorkItemList,
        )
        self.linked_assets = WorkItemLinkedassetsAPI(client)

    def apply(self, work_item: WorkItemApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = work_item.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(WorkItemApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(WorkItemApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WorkItem:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WorkItemList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> WorkItem | WorkItemList:
        if isinstance(external_id, str):
            work_item = self._retrieve((self.sources.space, external_id))

            linked_asset_edges = self.linked_assets.retrieve(external_id)
            work_item.linked_assets = [edge.end_node.external_id for edge in linked_asset_edges]

            return work_item
        else:
            work_items = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            linked_asset_edges = self.linked_assets.retrieve(external_id)
            self._set_linked_assets(work_items, linked_asset_edges)

            return work_items

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> WorkItemList:
        work_items = self._list(limit=limit)

        linked_asset_edges = self.linked_assets.list(limit=-1)
        self._set_linked_assets(work_items, linked_asset_edges)

        return work_items

    @staticmethod
    def _set_linked_assets(work_items: Sequence[WorkItem], linked_asset_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in linked_asset_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for work_item in work_items:
            node_id = work_item.id_tuple()
            if node_id in edges_by_start_node:
                work_item.linked_assets = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
