from __future__ import annotations

import datetime
from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._asset import AssetApply
    from ._work_item import WorkItemApply

__all__ = [
    "WorkOrder",
    "WorkOrderApply",
    "WorkOrderList",
    "WorkOrderApplyList",
    "WorkOrderFields",
    "WorkOrderTextFields",
]


WorkOrderTextFields = Literal[
    "description",
    "priority_description",
    "program_number",
    "status",
    "title",
    "work_order_number",
    "work_package_number",
]
WorkOrderFields = Literal[
    "actual_hours",
    "created_date",
    "description",
    "due_date",
    "duration_hours",
    "end_time",
    "is_active",
    "is_cancelled",
    "is_completed",
    "is_safety_critical",
    "percentage_progress",
    "planned_start",
    "priority_description",
    "program_number",
    "start_time",
    "status",
    "title",
    "work_order_number",
    "work_package_number",
]

_WORKORDER_PROPERTIES_BY_FIELD = {
    "actual_hours": "actualHours",
    "created_date": "createdDate",
    "description": "description",
    "due_date": "dueDate",
    "duration_hours": "durationHours",
    "end_time": "endTime",
    "is_active": "isActive",
    "is_cancelled": "isCancelled",
    "is_completed": "isCompleted",
    "is_safety_critical": "isSafetyCritical",
    "percentage_progress": "percentageProgress",
    "planned_start": "plannedStart",
    "priority_description": "priorityDescription",
    "program_number": "programNumber",
    "start_time": "startTime",
    "status": "status",
    "title": "title",
    "work_order_number": "workOrderNumber",
    "work_package_number": "workPackageNumber",
}


class WorkOrder(DomainModel):
    space: str = "tutorial_apm_simple"
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
    linked_assets: Optional[list[str]] = Field(None, alias="linkedAssets")
    percentage_progress: Optional[int] = Field(None, alias="percentageProgress")
    planned_start: Optional[datetime.datetime] = Field(None, alias="plannedStart")
    priority_description: Optional[str] = Field(None, alias="priorityDescription")
    program_number: Optional[str] = Field(None, alias="programNumber")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    status: Optional[str] = None
    title: Optional[str] = None
    work_items: Optional[list[str]] = Field(None, alias="workItems")
    work_order_number: Optional[str] = Field(None, alias="workOrderNumber")
    work_package_number: Optional[str] = Field(None, alias="workPackageNumber")

    def as_apply(self) -> WorkOrderApply:
        return WorkOrderApply(
            space=self.space,
            external_id=self.external_id,
            actual_hours=self.actual_hours,
            created_date=self.created_date,
            description=self.description,
            due_date=self.due_date,
            duration_hours=self.duration_hours,
            end_time=self.end_time,
            is_active=self.is_active,
            is_cancelled=self.is_cancelled,
            is_completed=self.is_completed,
            is_safety_critical=self.is_safety_critical,
            linked_assets=self.linked_assets,
            percentage_progress=self.percentage_progress,
            planned_start=self.planned_start,
            priority_description=self.priority_description,
            program_number=self.program_number,
            start_time=self.start_time,
            status=self.status,
            title=self.title,
            work_items=self.work_items,
            work_order_number=self.work_order_number,
            work_package_number=self.work_package_number,
        )


class WorkOrderApply(DomainModelApply):
    space: str = "tutorial_apm_simple"
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
    linked_assets: Union[list[AssetApply], list[str], None] = Field(default=None, repr=False, alias="linkedAssets")
    percentage_progress: Optional[int] = Field(None, alias="percentageProgress")
    planned_start: Optional[datetime.datetime] = Field(None, alias="plannedStart")
    priority_description: Optional[str] = Field(None, alias="priorityDescription")
    program_number: Optional[str] = Field(None, alias="programNumber")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    status: Optional[str] = None
    title: Optional[str] = None
    work_items: Union[list[WorkItemApply], list[str], None] = Field(default=None, repr=False, alias="workItems")
    work_order_number: Optional[str] = Field(None, alias="workOrderNumber")
    work_package_number: Optional[str] = Field(None, alias="workPackageNumber")

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.actual_hours is not None:
            properties["actualHours"] = self.actual_hours
        if self.created_date is not None:
            properties["createdDate"] = self.created_date.isoformat(timespec="milliseconds")
        if self.description is not None:
            properties["description"] = self.description
        if self.due_date is not None:
            properties["dueDate"] = self.due_date.isoformat(timespec="milliseconds")
        if self.duration_hours is not None:
            properties["durationHours"] = self.duration_hours
        if self.end_time is not None:
            properties["endTime"] = self.end_time.isoformat(timespec="milliseconds")
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
            properties["plannedStart"] = self.planned_start.isoformat(timespec="milliseconds")
        if self.priority_description is not None:
            properties["priorityDescription"] = self.priority_description
        if self.program_number is not None:
            properties["programNumber"] = self.program_number
        if self.start_time is not None:
            properties["startTime"] = self.start_time.isoformat(timespec="milliseconds")
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
                source=write_view or dm.ViewId("tutorial_apm_simple", "WorkOrder", "6f36e59c3c4896"),
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
                instances = linked_asset._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for work_item in self.work_items or []:
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

    def _create_work_item_edge(self, work_item: Union[str, WorkItemApply]) -> dm.EdgeApply:
        if isinstance(work_item, str):
            end_node_ext_id = work_item
        elif isinstance(work_item, DomainModelApply):
            end_node_ext_id = work_item.external_id
        else:
            raise TypeError(f"Expected str or WorkItemApply, got {type(work_item)}")

        return dm.EdgeApply(
            space="tutorial_apm_simple",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.workItems"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("tutorial_apm_simple", end_node_ext_id),
        )


class WorkOrderList(TypeList[WorkOrder]):
    _NODE = WorkOrder

    def as_apply(self) -> WorkOrderApplyList:
        return WorkOrderApplyList([node.as_apply() for node in self.data])


class WorkOrderApplyList(TypeApplyList[WorkOrderApply]):
    _NODE = WorkOrderApply
