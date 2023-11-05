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
    """This represent a read version of work item.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work item.
        criticality: The criticality field.
        description: The description field.
        is_completed: @name completed
        item_info: @name item information
        item_name: @name item name
        linked_assets: The linked asset field.
        method: The method field.
        title: The title field.
        to_be_done: @name to be done
        work_order: The work order field.
        created_time: The created time of the work item node.
        last_updated_time: The last updated time of the work item node.
        deleted_time: If present, the deleted time of the work item node.
        version: The version of the work item node.
    """

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
        """Convert this read version of work item to a write version."""
        return WorkItemApply(
            space=self.space,
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
    """This represent a write version of work item.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work item.
        criticality: The criticality field.
        description: The description field.
        is_completed: @name completed
        item_info: @name item information
        item_name: @name item name
        linked_assets: The linked asset field.
        method: The method field.
        title: The title field.
        to_be_done: @name to be done
        work_order: The work order field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

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

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
                source=write_view or dm.ViewId("tutorial_apm_simple", "WorkItem", "18ac48abbe96aa"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
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
                instances = linked_asset._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.work_order, DomainModelApply):
            instances = self.work_order._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_linked_asset_edge(self, linked_asset: Union[str, AssetApply]) -> dm.EdgeApply:
        if isinstance(linked_asset, str):
            end_space, end_node_ext_id = self.space, linked_asset
        elif isinstance(linked_asset, DomainModelApply):
            end_space, end_node_ext_id = linked_asset.space, linked_asset.external_id
        else:
            raise TypeError(f"Expected str or AssetApply, got {type(linked_asset)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("tutorial_apm_simple", "WorkItem.linkedAssets"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class WorkItemList(TypeList[WorkItem]):
    """List of work items in read version."""

    _NODE = WorkItem

    def as_apply(self) -> WorkItemApplyList:
        """Convert this read version of work item to a write version."""
        return WorkItemApplyList([node.as_apply() for node in self.data])


class WorkItemApplyList(TypeApplyList[WorkItemApply]):
    """List of work items in write version."""

    _NODE = WorkItemApply
