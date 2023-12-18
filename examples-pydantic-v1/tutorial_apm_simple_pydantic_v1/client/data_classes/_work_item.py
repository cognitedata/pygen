from __future__ import annotations

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
    from ._work_order import WorkOrder, WorkOrderApply


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
    """This represents the reading version of work item.

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

    space: str = DEFAULT_INSTANCE_SPACE
    criticality: Optional[str] = None
    description: Optional[str] = None
    is_completed: Optional[bool] = Field(None, alias="isCompleted")
    item_info: Optional[str] = Field(None, alias="itemInfo")
    item_name: Optional[str] = Field(None, alias="itemName")
    linked_assets: Union[list[Asset], list[str], None] = Field(default=None, repr=False, alias="linkedAssets")
    method: Optional[str] = None
    title: Optional[str] = None
    to_be_done: Optional[bool] = Field(None, alias="toBeDone")
    work_order: Union[WorkOrder, str, dm.NodeId, None] = Field(None, repr=False, alias="workOrder")

    def as_apply(self) -> WorkItemApply:
        """Convert this read version of work item to the writing version."""
        return WorkItemApply(
            space=self.space,
            external_id=self.external_id,
            criticality=self.criticality,
            description=self.description,
            is_completed=self.is_completed,
            item_info=self.item_info,
            item_name=self.item_name,
            linked_assets=[
                linked_asset.as_apply() if isinstance(linked_asset, DomainModel) else linked_asset
                for linked_asset in self.linked_assets or []
            ],
            method=self.method,
            title=self.title,
            to_be_done=self.to_be_done,
            work_order=self.work_order.as_apply() if isinstance(self.work_order, DomainModel) else self.work_order,
        )


class WorkItemApply(DomainModelApply):
    """This represents the writing version of work item.

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
        existing_version: Fail the ingestion request if the work item version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    criticality: Optional[str] = None
    description: Optional[str] = None
    is_completed: Optional[bool] = Field(None, alias="isCompleted")
    item_info: Optional[str] = Field(None, alias="itemInfo")
    item_name: Optional[str] = Field(None, alias="itemName")
    linked_assets: Union[list[AssetApply], list[str], None] = Field(default=None, repr=False, alias="linkedAssets")
    method: Optional[str] = None
    title: Optional[str] = None
    to_be_done: Optional[bool] = Field(None, alias="toBeDone")
    work_order: Union[WorkOrderApply, str, dm.NodeId, None] = Field(None, repr=False, alias="workOrder")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "tutorial_apm_simple", "WorkItem", "18ac48abbe96aa"
        )

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
                "space": self.space if isinstance(self.work_order, str) else self.work_order.space,
                "externalId": self.work_order if isinstance(self.work_order, str) else self.work_order.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("tutorial_apm_simple", "WorkItem"),
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("tutorial_apm_simple", "WorkItem.linkedAssets")
        for linked_asset in self.linked_assets or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, linked_asset, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.work_order, DomainModelApply):
            other_resources = self.work_order._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class WorkItemList(DomainModelList[WorkItem]):
    """List of work items in the read version."""

    _INSTANCE = WorkItem

    def as_apply(self) -> WorkItemApplyList:
        """Convert these read versions of work item to the writing versions."""
        return WorkItemApplyList([node.as_apply() for node in self.data])


class WorkItemApplyList(DomainModelApplyList[WorkItemApply]):
    """List of work items in the writing version."""

    _INSTANCE = WorkItemApply


def _create_work_item_filter(
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
    work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if criticality is not None and isinstance(criticality, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("criticality"), value=criticality))
    if criticality and isinstance(criticality, list):
        filters.append(dm.filters.In(view_id.as_property_ref("criticality"), values=criticality))
    if criticality_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("criticality"), value=criticality_prefix))
    if description is not None and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if is_completed is not None and isinstance(is_completed, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCompleted"), value=is_completed))
    if item_info is not None and isinstance(item_info, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("itemInfo"), value=item_info))
    if item_info and isinstance(item_info, list):
        filters.append(dm.filters.In(view_id.as_property_ref("itemInfo"), values=item_info))
    if item_info_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("itemInfo"), value=item_info_prefix))
    if item_name is not None and isinstance(item_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("itemName"), value=item_name))
    if item_name and isinstance(item_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("itemName"), values=item_name))
    if item_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("itemName"), value=item_name_prefix))
    if method is not None and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if title is not None and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if to_be_done is not None and isinstance(to_be_done, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("toBeDone"), value=to_be_done))
    if work_order and isinstance(work_order, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("workOrder"), value={"space": "tutorial_apm_simple", "externalId": work_order}
            )
        )
    if work_order and isinstance(work_order, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("workOrder"), value={"space": work_order[0], "externalId": work_order[1]}
            )
        )
    if work_order and isinstance(work_order, list) and isinstance(work_order[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("workOrder"),
                values=[{"space": "tutorial_apm_simple", "externalId": item} for item in work_order],
            )
        )
    if work_order and isinstance(work_order, list) and isinstance(work_order[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("workOrder"),
                values=[{"space": item[0], "externalId": item[1]} for item in work_order],
            )
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
