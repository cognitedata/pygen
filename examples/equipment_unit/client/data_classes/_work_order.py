from __future__ import annotations

from typing import Literal, Optional

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


__all__ = [
    "WorkOrder",
    "WorkOrderApply",
    "WorkOrderList",
    "WorkOrderApplyList",
    "WorkOrderFields",
    "WorkOrderTextFields",
]


WorkOrderTextFields = Literal["description", "performed_by", "type_"]
WorkOrderFields = Literal["description", "performed_by", "type_"]

_WORKORDER_PROPERTIES_BY_FIELD = {
    "description": "description",
    "performed_by": "performedBy",
    "type_": "type",
}


class WorkOrder(DomainModel):
    """This represents the reading version of work order.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work order.
        description: The description field.
        performed_by: The performed by field.
        type_: The type field.
        created_time: The created time of the work order node.
        last_updated_time: The last updated time of the work order node.
        deleted_time: If present, the deleted time of the work order node.
        version: The version of the work order node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    description: Optional[str] = None
    performed_by: Optional[str] = Field(None, alias="performedBy")
    type_: Optional[str] = Field(None, alias="type")

    def as_apply(self) -> WorkOrderApply:
        """Convert this read version of work order to the writing version."""
        return WorkOrderApply(
            space=self.space,
            external_id=self.external_id,
            description=self.description,
            performed_by=self.performed_by,
            type_=self.type_,
        )


class WorkOrderApply(DomainModelApply):
    """This represents the writing version of work order.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work order.
        description: The description field.
        performed_by: The performed by field.
        type_: The type field.
        existing_version: Fail the ingestion request if the work order version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    description: Optional[str] = None
    performed_by: Optional[str] = Field(None, alias="performedBy")
    type_: Optional[str] = Field(None, alias="type")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "WorkOrder", "c5543fb2b1bc81"
        )

        properties = {}

        if self.description is not None:
            properties["description"] = self.description

        if self.performed_by is not None:
            properties["performedBy"] = self.performed_by

        if self.type_ is not None:
            properties["type"] = self.type_

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

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
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    performed_by: str | list[str] | None = None,
    performed_by_prefix: str | None = None,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if description is not None and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if performed_by is not None and isinstance(performed_by, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("performedBy"), value=performed_by))
    if performed_by and isinstance(performed_by, list):
        filters.append(dm.filters.In(view_id.as_property_ref("performedBy"), values=performed_by))
    if performed_by_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("performedBy"), value=performed_by_prefix))
    if type_ is not None and isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
