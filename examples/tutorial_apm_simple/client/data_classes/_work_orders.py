from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from ._assets import AssetApply
    from ._work_items import WorkApply

__all__ = ["Work", "WorkApply", "WorkList"]


class Work(DomainModel):
    space: ClassVar[str] = "tutorial_apm_simple"
    actual_hours: Optional[int] = Field(None, alias="actualHours")
    created_date: Optional[datetime.datetime] = Field(None, alias="createdDate")
    description: Optional[str] = None
    due_date: Optional[datetime.datetime] = Field(None, alias="dueDate")
    duration_hours: Optional[int] = Field(None, alias="durationHours")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    is_active: Optional[bool] = Field(None, alias="isActive")
    is_cancelled: Optional[bool] = Field(None, alias="isCancelled")
    is_completed: Optional[bool] = Field(None, alias="isCompleted")
    is_safety_critical: Optional[bool] = Field(None, alias="isSafetyCritical")
    linked_assets: list[str] = []
    percentage_progress: Optional[int] = Field(None, alias="percentageProgress")
    planned_start: Optional[datetime.datetime] = Field(None, alias="plannedStart")
    priority_description: Optional[str] = Field(None, alias="priorityDescription")
    program_number: Optional[str] = Field(None, alias="programNumber")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    status: Optional[str] = None
    title: Optional[str] = None
    work_items: list[str] = []
    work_order_number: Optional[str] = Field(None, alias="workOrderNumber")
    work_package_number: Optional[str] = Field(None, alias="workPackageNumber")


class WorkApply(DomainModelApply):
    space: ClassVar[str] = "tutorial_apm_simple"
    actual_hours: Optional[int] = None
    created_date: Optional[datetime.datetime] = None
    description: Optional[str] = None
    due_date: Optional[datetime.datetime] = None
    duration_hours: Optional[int] = None
    end_time: Optional[datetime.datetime] = None
    is_active: Optional[bool] = None
    is_cancelled: Optional[bool] = None
    is_completed: Optional[bool] = None
    is_safety_critical: Optional[bool] = None
    linked_assets: Union[list[AssetApply], list[str]] = Field(default_factory=list, repr=False)
    percentage_progress: Optional[int] = None
    planned_start: Optional[datetime.datetime] = None
    priority_description: Optional[str] = None
    program_number: Optional[str] = None
    start_time: Optional[datetime.datetime] = None
    status: Optional[str] = None
    title: Optional[str] = None
    work_items: Union[list[WorkApply], list[str]] = Field(default_factory=list, repr=False)
    work_order_number: Optional[str] = None
    work_package_number: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.actual_hours is not None:
            properties["actualHours"] = self.actual_hours
        if self.created_date is not None:
            properties["createdDate"] = self.created_date.isoformat()
        if self.description is not None:
            properties["description"] = self.description
        if self.due_date is not None:
            properties["dueDate"] = self.due_date.isoformat()
        if self.duration_hours is not None:
            properties["durationHours"] = self.duration_hours
        if self.end_time is not None:
            properties["endTime"] = self.end_time.isoformat()
        if self.is_active is not None:
            properties["isActive"] = self.is_active
        if self.is_cancelled is not None:
            properties["isCancelled"] = self.is_cancelled
        if self.is_completed is not None:
            properties["isCompleted"] = self.is_completed
        if self.is_safety_critical is not None:
            properties["isSafetyCritical"] = self.is_safety_critical
        if self.percentage_progress is not None:
            properties["percentageProgress"] = self.percentage_progress
        if self.planned_start is not None:
            properties["plannedStart"] = self.planned_start.isoformat()
        if self.priority_description is not None:
            properties["priorityDescription"] = self.priority_description
        if self.program_number is not None:
            properties["programNumber"] = self.program_number
        if self.start_time is not None:
            properties["startTime"] = self.start_time.isoformat()
        if self.status is not None:
            properties["status"] = self.status
        if self.title is not None:
            properties["title"] = self.title
        if self.work_order_number is not None:
            properties["workOrderNumber"] = self.work_order_number
        if self.work_package_number is not None:
            properties["workPackageNumber"] = self.work_package_number
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("tutorial_apm_simple", "WorkOrder"),
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

        for linked_asset in self.linked_assets:
            edge = self._create_linked_asset_edge(linked_asset)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(linked_asset, DomainModelApply):
                instances = linked_asset._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for work_item in self.work_items:
            edge = self._create_work_item_edge(work_item)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(work_item, DomainModelApply):
                instances = work_item._to_instances_apply(cache)
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
            type=dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.linkedAssets"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("tutorial_apm_simple", end_node_ext_id),
        )

    def _create_work_item_edge(self, work_item: Union[str, WorkApply]) -> dm.EdgeApply:
        if isinstance(work_item, str):
            end_node_ext_id = work_item
        elif isinstance(work_item, DomainModelApply):
            end_node_ext_id = work_item.external_id
        else:
            raise TypeError(f"Expected str or WorkApply, got {type(work_item)}")

        return dm.EdgeApply(
            space="tutorial_apm_simple",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.workItems"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("tutorial_apm_simple", end_node_ext_id),
        )


class WorkList(TypeList[Work]):
    _NODE = Work
