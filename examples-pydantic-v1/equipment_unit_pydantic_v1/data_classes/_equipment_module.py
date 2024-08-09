from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries
from pydantic import Field
from pydantic import validator, root_validator

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
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
)


__all__ = [
    "EquipmentModule",
    "EquipmentModuleWrite",
    "EquipmentModuleApply",
    "EquipmentModuleList",
    "EquipmentModuleWriteList",
    "EquipmentModuleApplyList",
    "EquipmentModuleFields",
    "EquipmentModuleTextFields",
    "EquipmentModuleGraphQL",
]


EquipmentModuleTextFields = Literal["description", "name", "sensor_value", "type_"]
EquipmentModuleFields = Literal["description", "name", "sensor_value", "type_"]

_EQUIPMENTMODULE_PROPERTIES_BY_FIELD = {
    "description": "description",
    "name": "name",
    "sensor_value": "sensor_value",
    "type_": "type",
}


class EquipmentModuleGraphQL(GraphQLCore):
    """This represents the reading version of equipment module, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the equipment module.
        data_record: The data record of the equipment module node.
        description: The description field.
        name: The name field.
        sensor_value: The sensor value field.
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33")
    description: Optional[str] = None
    name: Optional[str] = None
    sensor_value: Union[TimeSeries, dict, None] = None
    type_: Optional[str] = Field(None, alias="type")

    @root_validator(pre=True)
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @validator("sensor_value", pre=True)
    def parse_timeseries(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [TimeSeries.load(v) if isinstance(v, dict) else v for v in value]
        elif isinstance(value, dict):
            return TimeSeries.load(value)
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> EquipmentModule:
        """Convert this GraphQL format of equipment module to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return EquipmentModule(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            description=self.description,
            name=self.name,
            sensor_value=self.sensor_value,
            type_=self.type_,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> EquipmentModuleWrite:
        """Convert this GraphQL format of equipment module to the writing format."""
        return EquipmentModuleWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            description=self.description,
            name=self.name,
            sensor_value=self.sensor_value,
            type_=self.type_,
        )


class EquipmentModule(DomainModel):
    """This represents the reading version of equipment module.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the equipment module.
        data_record: The data record of the equipment module node.
        description: The description field.
        name: The name field.
        sensor_value: The sensor value field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    description: Optional[str] = None
    name: Optional[str] = None
    sensor_value: Union[TimeSeries, str, None] = None
    type_: Optional[str] = Field(None, alias="type")

    def as_write(self) -> EquipmentModuleWrite:
        """Convert this read version of equipment module to the writing version."""
        return EquipmentModuleWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            description=self.description,
            name=self.name,
            sensor_value=self.sensor_value,
            type_=self.type_,
        )

    def as_apply(self) -> EquipmentModuleWrite:
        """Convert this read version of equipment module to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class EquipmentModuleWrite(DomainModelWrite):
    """This represents the writing version of equipment module.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the equipment module.
        data_record: The data record of the equipment module node.
        description: The description field.
        name: The name field.
        sensor_value: The sensor value field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    description: Optional[str] = None
    name: Optional[str] = None
    sensor_value: Union[TimeSeries, str, None] = None
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

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.sensor_value is not None or write_none:
            properties["sensor_value"] = (
                self.sensor_value
                if isinstance(self.sensor_value, str) or self.sensor_value is None
                else self.sensor_value.external_id
            )

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

        if isinstance(self.sensor_value, TimeSeries):
            resources.time_series.append(self.sensor_value)

        return resources


class EquipmentModuleApply(EquipmentModuleWrite):
    def __new__(cls, *args, **kwargs) -> EquipmentModuleApply:
        warnings.warn(
            "EquipmentModuleApply is deprecated and will be removed in v1.0. Use EquipmentModuleWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "EquipmentModule.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class EquipmentModuleList(DomainModelList[EquipmentModule]):
    """List of equipment modules in the read version."""

    _INSTANCE = EquipmentModule

    def as_write(self) -> EquipmentModuleWriteList:
        """Convert these read versions of equipment module to the writing versions."""
        return EquipmentModuleWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> EquipmentModuleWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class EquipmentModuleWriteList(DomainModelWriteList[EquipmentModuleWrite]):
    """List of equipment modules in the writing version."""

    _INSTANCE = EquipmentModuleWrite


class EquipmentModuleApplyList(EquipmentModuleWriteList): ...


def _create_equipment_module_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
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
