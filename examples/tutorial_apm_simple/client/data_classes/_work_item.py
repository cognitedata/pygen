from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._asset import AssetApply
    from ._work_order import WorkOrderApply

__all__ = ["WorkItem", "WorkItemApply", "WorkItemList", "WorkItemApplyList", "WorkItemFields", "WorkItemTextFields"]


WorkItemTextFields = Literal["criticality", "description", "item_info", "item_name", "method", "title"]
WorkItemFields = Literal[
    "criticality", "description", "is_completed", "item_info", "item_name", "method", "title", "to_be_done"
]

_WORKITEM_PROPERTIES_BY_FIELD = {
    "criticality": "criticality",
    "description": "description",
    "is_completed": "isCompleted",
    "item_info": "itemInfo",
    "item_name": "itemName",
    "method": "method",
    "title": "title",
    "to_be_done": "toBeDone",
}


class WorkItem(DomainModel):
    space: str = "tutorial_apm_simple"
    criticality: Optional[str] = None
    description: Optional[str] = None
    is_completed: Optional[bool] = Field(None, alias="isCompleted")
    item_info: Optional[str] = Field(None, alias="itemInfo")
    item_name: Optional[str] = Field(None, alias="itemName")
    linked_assets: Optional[list[str]] = Field(None, alias="linkedAssets")
    method: Optional[str] = None
    title: Optional[str] = None
    to_be_done: Optional[bool] = Field(None, alias="toBeDone")
    work_order: Optional[str] = Field(None, alias="workOrder")

    def as_apply(self) -> WorkItemApply:
        return WorkItemApply(
            external_id=self.external_id,
            criticality=self.criticality,
            description=self.description,
            is_completed=self.is_completed,
            item_info=self.item_info,
            item_name=self.item_name,
            linked_assets=self.linked_assets,
            method=self.method,
            title=self.title,
            to_be_done=self.to_be_done,
            work_order=self.work_order,
        )


class WorkItemApply(DomainModelApply):
    space: str = "tutorial_apm_simple"
    criticality: Optional[str] = None
    description: Optional[str] = None
    is_completed: Optional[bool] = Field(None, alias="isCompleted")
    item_info: Optional[str] = Field(None, alias="itemInfo")
    item_name: Optional[str] = Field(None, alias="itemName")
    linked_assets: Union[list[AssetApply], list[str], None] = Field(default=None, repr=False, alias="linkedAssets")
    method: Optional[str] = None
    title: Optional[str] = None
    to_be_done: Optional[bool] = Field(None, alias="toBeDone")
    work_order: Union[WorkOrderApply, str, None] = Field(None, repr=False, alias="workOrder")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.criticality is not None:
            properties["criticality"] = self.criticality
        if self.description is not None:
            properties["description"] = self.description
        if self.is_completed is not None:
            properties["isCompleted"] = self.is_completed
        if self.item_info is not None:
            properties["itemInfo"] = self.item_info
        if self.item_name is not None:
            properties["itemName"] = self.item_name
        if self.method is not None:
            properties["method"] = self.method
        if self.title is not None:
            properties["title"] = self.title
        if self.to_be_done is not None:
            properties["toBeDone"] = self.to_be_done
        if self.work_order is not None:
            properties["workOrder"] = {
                "space": "tutorial_apm_simple",
                "externalId": self.work_order if isinstance(self.work_order, str) else self.work_order.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("tutorial_apm_simple", "WorkItem"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for linked_asset in self.linked_assets or []:
            edge = self._create_linked_asset_edge(linked_asset)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(linked_asset, DomainModelApply):
                instances = linked_asset._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.work_order, DomainModelApply):
            instances = self.work_order._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_linked_asset_edge(self, linked_asset: Union[str, AssetApply]) -> dm.EdgeApply:
        if isinstance(linked_asset, str):
            end_node_ext_id = linked_asset
        elif isinstance(linked_asset, DomainModelApply):
            end_node_ext_id = linked_asset.external_id
        else:
            raise TypeError(f"Expected str or AssetApply, got {type(linked_asset)}")

        return dm.EdgeApply(
            space="tutorial_apm_simple",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("tutorial_apm_simple", "WorkItem.linkedAssets"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("tutorial_apm_simple", end_node_ext_id),
        )


class WorkItemList(TypeList[WorkItem]):
    _NODE = WorkItem

    def as_apply(self) -> WorkItemApplyList:
        return WorkItemApplyList([node.as_apply() for node in self.data])


class WorkItemApplyList(TypeApplyList[WorkItemApply]):
    _NODE = WorkItemApply
