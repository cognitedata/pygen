from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    as_node_id,
)


__all__ = [
    "WorkOrder",
    "WorkOrderWrite",
    "WorkOrderApply",
    "WorkOrderList",
    "WorkOrderWriteList",
    "WorkOrderApplyList",
    "WorkOrderFields",
    "WorkOrderTextFields",
    "WorkOrderGraphQL",
]


WorkOrderTextFields = Literal["description", "performed_by", "type_"]
WorkOrderFields = Literal["description", "performed_by", "type_"]

_WORKORDER_PROPERTIES_BY_FIELD = {
    "description": "description",
    "performed_by": "performedBy",
    "type_": "type",
}


class WorkOrderGraphQL(GraphQLCore):
    """This represents the reading version of work order, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work order.
        data_record: The data record of the work order node.
        description: The description field.
        performed_by: The performed by field.
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "WorkOrder", "c5543fb2b1bc81")
    description: Optional[str] = None
    performed_by: Optional[str] = Field(None, alias="performedBy")
    type_: Optional[str] = Field(None, alias="type")

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> WorkOrder:
        """Convert this GraphQL format of work order to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return WorkOrder(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            description=self.description,
            performed_by=self.performed_by,
            type_=self.type_,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> WorkOrderWrite:
        """Convert this GraphQL format of work order to the writing format."""
        return WorkOrderWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            description=self.description,
            performed_by=self.performed_by,
            type_=self.type_,
        )


class WorkOrder(DomainModel):
    """This represents the reading version of work order.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work order.
        data_record: The data record of the work order node.
        description: The description field.
        performed_by: The performed by field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "WorkOrder", "c5543fb2b1bc81")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    description: Optional[str] = None
    performed_by: Optional[str] = Field(None, alias="performedBy")
    type_: Optional[str] = Field(None, alias="type")

    def as_write(self) -> WorkOrderWrite:
        """Convert this read version of work order to the writing version."""
        return WorkOrderWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            description=self.description,
            performed_by=self.performed_by,
            type_=self.type_,
        )

    def as_apply(self) -> WorkOrderWrite:
        """Convert this read version of work order to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WorkOrderWrite(DomainModelWrite):
    """This represents the writing version of work order.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the work order.
        data_record: The data record of the work order node.
        description: The description field.
        performed_by: The performed by field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "WorkOrder", "c5543fb2b1bc81")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    description: Optional[str] = None
    performed_by: Optional[str] = Field(None, alias="performedBy")
    type_: Optional[str] = Field(None, alias="type")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.performed_by is not None or write_none:
            properties["performedBy"] = self.performed_by

        if self.type_ is not None or write_none:
            properties["type"] = self.type_

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class WorkOrderApply(WorkOrderWrite):
    def __new__(cls, *args, **kwargs) -> WorkOrderApply:
        warnings.warn(
            "WorkOrderApply is deprecated and will be removed in v1.0. Use WorkOrderWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WorkOrder.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WorkOrderList(DomainModelList[WorkOrder]):
    """List of work orders in the read version."""

    _INSTANCE = WorkOrder

    def as_write(self) -> WorkOrderWriteList:
        """Convert these read versions of work order to the writing versions."""
        return WorkOrderWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WorkOrderWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WorkOrderWriteList(DomainModelWriteList[WorkOrderWrite]):
    """List of work orders in the writing version."""

    _INSTANCE = WorkOrderWrite


class WorkOrderApplyList(WorkOrderWriteList): ...


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
    filters: list[dm.Filter] = []
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(performed_by, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("performedBy"), value=performed_by))
    if performed_by and isinstance(performed_by, list):
        filters.append(dm.filters.In(view_id.as_property_ref("performedBy"), values=performed_by))
    if performed_by_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("performedBy"), value=performed_by_prefix))
    if isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
