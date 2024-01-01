from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = [
    "EquipmentModule",
    "EquipmentModuleApply",
    "EquipmentModuleList",
    "EquipmentModuleApplyList",
    "EquipmentModuleFields",
    "EquipmentModuleTextFields",
]


EquipmentModuleTextFields = Literal["description", "name", "sensor_value", "type_"]
EquipmentModuleFields = Literal["description", "name", "sensor_value", "type_"]

_EQUIPMENTMODULE_PROPERTIES_BY_FIELD = {
    "description": "description",
    "name": "name",
    "sensor_value": "sensor_value",
    "type_": "type",
}


class EquipmentModule(DomainModel):
    """This represents the reading version of equipment module.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the equipment module.
        description: The description field.
        name: The name field.
        sensor_value: The sensor value field.
        type_: The type field.
        created_time: The created time of the equipment module node.
        last_updated_time: The last updated time of the equipment module node.
        deleted_time: If present, the deleted time of the equipment module node.
        version: The version of the equipment module node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    description: Optional[str] = None
    name: Optional[str] = None
    sensor_value: Union[TimeSeries, str, None] = None
    type_: Optional[str] = Field(None, alias="type")

    def as_apply(self) -> EquipmentModuleApply:
        """Convert this read version of equipment module to the writing version."""
        return EquipmentModuleApply(
            space=self.space,
            external_id=self.external_id,
            description=self.description,
            name=self.name,
            sensor_value=self.sensor_value,
            type_=self.type_,
        )


class EquipmentModuleApply(DomainModelApply):
    """This represents the writing version of equipment module.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the equipment module.
        description: The description field.
        name: The name field.
        sensor_value: The sensor value field.
        type_: The type field.
        existing_version: Fail the ingestion request if the equipment module version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    description: Optional[str] = None
    name: Optional[str] = None
    sensor_value: Union[TimeSeries, str, None] = None
    type_: Optional[str] = Field(None, alias="type")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            EquipmentModule, dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33")
        )

        properties: dict[str, Any] = {}

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.sensor_value is not None or write_none:
            if isinstance(self.sensor_value, str) or self.sensor_value is None:
                properties["sensor_value"] = self.sensor_value
            else:
                properties["sensor_value"] = self.sensor_value.external_id

        if self.type_ is not None or write_none:
            properties["type"] = self.type_

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.sensor_value, CogniteTimeSeries):
            resources.time_series.append(self.sensor_value)

        return resources


class EquipmentModuleList(DomainModelList[EquipmentModule]):
    """List of equipment modules in the read version."""

    _INSTANCE = EquipmentModule

    def as_apply(self) -> EquipmentModuleApplyList:
        """Convert these read versions of equipment module to the writing versions."""
        return EquipmentModuleApplyList([node.as_apply() for node in self.data])


class EquipmentModuleApplyList(DomainModelApplyList[EquipmentModuleApply]):
    """List of equipment modules in the writing version."""

    _INSTANCE = EquipmentModuleApply


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
    filters = []
    if description is not None and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
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
