from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client import data_classes
from pydantic import Field
from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelList,
    DomainModelApplyList,
    DomainsApply,
    DomainRelationApply,
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
    """This represent a read version of equipment module.

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

    space: str = "IntegrationTestsImmutable"
    description: Optional[str] = None
    name: Optional[str] = None
    sensor_value: Union[TimeSeries, str, None] = None
    type_: Optional[str] = Field(None, alias="type")

    def as_apply(self) -> EquipmentModuleApply:
        """Convert this read version of equipment module to a write version."""
        return EquipmentModuleApply(
            space=self.space,
            external_id=self.external_id,
            description=self.description,
            name=self.name,
            sensor_value=self.sensor_value,
            type_=self.type_,
        )


class EquipmentModuleApply(DomainModelApply):
    """This represent a write version of equipment module.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the equipment module.
        description: The description field.
        name: The name field.
        sensor_value: The sensor value field.
        type_: The type field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    description: Optional[str] = None
    name: Optional[str] = None
    sensor_value: Union[TimeSeries, str, None] = None
    type_: Optional[str] = Field(None, alias="type")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> DomainsApply:
        this_instances = DomainsApply()
        if self.external_id in cache:
            return this_instances
        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33"
        )

        properties = {}
        if self.description is not None:
            properties["description"] = self.description
        if self.name is not None:
            properties["name"] = self.name
        if self.sensor_value is not None:
            properties["sensor_value"] = (
                self.sensor_value.external_id
                if isinstance(self.sensor_value, data_classes.TimeSeries)
                else self.sensor_value
            )
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
            this_instances.nodes.append(this_node)
            cache.add((self.space, self.external_id))

        if isinstance(self.sensor_value, data_classes.TimeSeries):
            this_instances.time_series.append(self.sensor_value)
            cache.add(("", self.sensor_value.external_id))

        return this_instances


class EquipmentModuleList(DomainModelList[EquipmentModule]):
    """List of equipment modules in read version."""

    _INSTANCE = EquipmentModule

    def as_apply(self) -> EquipmentModuleApplyList:
        """Convert this read version of equipment module to a write version."""
        return EquipmentModuleApplyList([node.as_apply() for node in self.data])


class EquipmentModuleApplyList(DomainModelApplyList[EquipmentModuleApply]):
    """List of equipment modules in write version."""

    _INSTANCE = EquipmentModuleApply
