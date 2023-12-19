from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._asset import Asset, AssetApply
    from ._work_item import WorkItem, WorkItemApply


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
    """This represents the reading version of work order.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work order.
        actual_hours: @name actual (hours)
        created_date: @name created date
        description: The description field.
        due_date: @name due date
        duration_hours: @name duration (hours)
        end_time: @name end time
        is_active: @name active
        is_cancelled: @name cancelled
        is_completed: @name completed
        is_safety_critical: @name safety critical
        linked_assets: The linked asset field.
        percentage_progress: @name percentage progress
        planned_start: @name planned start
        priority_description: @name priority description
        program_number: @name program number
        start_time: @name start time
        status: The status field.
        title: The title field.
        work_items: The work item field.
        work_order_number: @name work order number
        work_package_number: @name work package number
        created_time: The created time of the work order node.
        last_updated_time: The last updated time of the work order node.
        deleted_time: If present, the deleted time of the work order node.
        version: The version of the work order node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
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
    linked_assets: Union[list[Asset], list[str], None] = Field(default=None, repr=False, alias="linkedAssets")
    percentage_progress: Optional[int] = Field(None, alias="percentageProgress")
    planned_start: Optional[datetime.datetime] = Field(None, alias="plannedStart")
    priority_description: Optional[str] = Field(None, alias="priorityDescription")
    program_number: Optional[str] = Field(None, alias="programNumber")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    status: Optional[str] = None
    title: Optional[str] = None
    work_items: Union[list[WorkItem], list[str], None] = Field(default=None, repr=False, alias="workItems")
    work_order_number: Optional[str] = Field(None, alias="workOrderNumber")
    work_package_number: Optional[str] = Field(None, alias="workPackageNumber")

    def as_apply(self) -> WorkOrderApply:
        """Convert this read version of work order to the writing version."""
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
            linked_assets=[
                linked_asset.as_apply() if isinstance(linked_asset, DomainModel) else linked_asset
                for linked_asset in self.linked_assets or []
            ],
            percentage_progress=self.percentage_progress,
            planned_start=self.planned_start,
            priority_description=self.priority_description,
            program_number=self.program_number,
            start_time=self.start_time,
            status=self.status,
            title=self.title,
            work_items=[
                work_item.as_apply() if isinstance(work_item, DomainModel) else work_item
                for work_item in self.work_items or []
            ],
            work_order_number=self.work_order_number,
            work_package_number=self.work_package_number,
        )


class WorkOrderApply(DomainModelApply):
    """This represents the writing version of work order.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work order.
        actual_hours: @name actual (hours)
        created_date: @name created date
        description: The description field.
        due_date: @name due date
        duration_hours: @name duration (hours)
        end_time: @name end time
        is_active: @name active
        is_cancelled: @name cancelled
        is_completed: @name completed
        is_safety_critical: @name safety critical
        linked_assets: The linked asset field.
        percentage_progress: @name percentage progress
        planned_start: @name planned start
        priority_description: @name priority description
        program_number: @name program number
        start_time: @name start time
        status: The status field.
        title: The title field.
        work_items: The work item field.
        work_order_number: @name work order number
        work_package_number: @name work package number
        existing_version: Fail the ingestion request if the work order version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
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

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "tutorial_apm_simple", "WorkOrder", "6f36e59c3c4896"
        )

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
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder"),
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.linkedAssets")
        for linked_asset in self.linked_assets or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=linked_asset,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.workItems")
        for work_item in self.work_items or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=work_item, edge_type=edge_type, view_by_write_class=view_by_write_class
            )
            resources.extend(other_resources)

        return resources


class WorkOrderList(DomainModelList[WorkOrder]):
    """List of work orders in the read version."""

    _INSTANCE = WorkOrder

    def as_apply(self) -> WorkOrderApplyList:
        """Convert these read versions of work order to the writing versions."""
        return WorkOrderApplyList([node.as_apply() for node in self.data])


class WorkOrderApplyList(DomainModelApplyList[WorkOrderApply]):
    """List of work orders in the writing version."""

    _INSTANCE = WorkOrderApply


def _create_work_order_filter(
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
    if description is not None and isinstance(description, str):
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
    if is_active is not None and isinstance(is_active, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isActive"), value=is_active))
    if is_cancelled is not None and isinstance(is_cancelled, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCancelled"), value=is_cancelled))
    if is_completed is not None and isinstance(is_completed, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCompleted"), value=is_completed))
    if is_safety_critical is not None and isinstance(is_safety_critical, bool):
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
    if priority_description is not None and isinstance(priority_description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priorityDescription"), value=priority_description))
    if priority_description and isinstance(priority_description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priorityDescription"), values=priority_description))
    if priority_description_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("priorityDescription"), value=priority_description_prefix)
        )
    if program_number is not None and isinstance(program_number, str):
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
    if status is not None and isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
    if status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("status"), value=status_prefix))
    if title is not None and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if work_order_number is not None and isinstance(work_order_number, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workOrderNumber"), value=work_order_number))
    if work_order_number and isinstance(work_order_number, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workOrderNumber"), values=work_order_number))
    if work_order_number_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("workOrderNumber"), value=work_order_number_prefix))
    if work_package_number is not None and isinstance(work_package_number, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workPackageNumber"), value=work_package_number))
    if work_package_number and isinstance(work_package_number, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workPackageNumber"), values=work_package_number))
    if work_package_number_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("workPackageNumber"), value=work_package_number_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
