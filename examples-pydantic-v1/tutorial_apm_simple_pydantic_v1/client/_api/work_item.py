from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple_pydantic_v1.client.data_classes import WorkItem, WorkItemApply, WorkItemList, WorkItemApplyList


class WorkItemLinkedAssetsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="tutorial_apm_simple") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WorkItem.linkedAssets"},
        )
        if isinstance(external_id, str):
            is_work_item = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_work_item))

        else:
            is_work_items = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_work_items)
            )

    def list(
        self, work_item_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="tutorial_apm_simple"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "WorkItem.linkedAssets"},
        )
        filters.append(is_edge_type)
        if work_item_id:
            work_item_ids = [work_item_id] if isinstance(work_item_id, str) else work_item_id
            is_work_items = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in work_item_ids],
            )
            filters.append(is_work_items)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WorkItemAPI(TypeAPI[WorkItem, WorkItemApply, WorkItemList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkItem,
            class_apply_type=WorkItemApply,
            class_list=WorkItemList,
        )
        self._view_id = view_id
        self.linked_assets = WorkItemLinkedAssetsAPI(client)

    def apply(
        self, work_item: WorkItemApply | Sequence[WorkItemApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(work_item, WorkItemApply):
            instances = work_item.to_instances_apply()
        else:
            instances = WorkItemApplyList(work_item).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="tutorial_apm_simple") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WorkItem:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WorkItemList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> WorkItem | WorkItemList:
        if isinstance(external_id, str):
            work_item = self._retrieve((self._sources.space, external_id))

            linked_asset_edges = self.linked_assets.retrieve(external_id)
            work_item.linked_assets = [edge.end_node.external_id for edge in linked_asset_edges]

            return work_item
        else:
            work_items = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            linked_asset_edges = self.linked_assets.retrieve(external_id)
            self._set_linked_assets(work_items, linked_asset_edges)

            return work_items

    def list(
        self,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WorkItemList:
        filter_ = _create_filter(
            self._view_id,
            criticality,
            criticality_prefix,
            description,
            description_prefix,
            is_completed,
            item_info,
            item_info_prefix,
            item_name,
            item_name_prefix,
            method,
            method_prefix,
            title,
            title_prefix,
            to_be_done,
            external_id_prefix,
            filter,
        )

        work_items = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            linked_asset_edges = self.linked_assets.list(work_items.as_external_ids(), limit=-1)
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


def _create_filter(
    view_id: dm.ViewId,
    criticality: str | list[str] | None = None,
    criticality_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    is_completed: bool | None = None,
    item_info: str | list[str] | None = None,
    item_info_prefix: str | None = None,
    item_name: str | list[str] | None = None,
    item_name_prefix: str | None = None,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    title: str | list[str] | None = None,
    title_prefix: str | None = None,
    to_be_done: bool | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if criticality and isinstance(criticality, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("criticality"), value=criticality))
    if criticality and isinstance(criticality, list):
        filters.append(dm.filters.In(view_id.as_property_ref("criticality"), values=criticality))
    if criticality_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("criticality"), value=criticality_prefix))
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if is_completed and isinstance(is_completed, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCompleted"), value=is_completed))
    if item_info and isinstance(item_info, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("itemInfo"), value=item_info))
    if item_info and isinstance(item_info, list):
        filters.append(dm.filters.In(view_id.as_property_ref("itemInfo"), values=item_info))
    if item_info_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("itemInfo"), value=item_info_prefix))
    if item_name and isinstance(item_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("itemName"), value=item_name))
    if item_name and isinstance(item_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("itemName"), values=item_name))
    if item_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("itemName"), value=item_name_prefix))
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if title and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if to_be_done and isinstance(to_be_done, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("toBeDone"), value=to_be_done))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
