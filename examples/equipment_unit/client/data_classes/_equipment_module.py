from __future__ import annotations

import datetime
from typing import Literal, Optional, TYPE_CHECKING

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._start_end_time import StartEndTimeApply

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
    sensor_value: Optional[str] = None
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
    sensor_value: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.description is not None:
            properties["description"] = self.description
        if self.name is not None:
            properties["name"] = self.name
        if self.sensor_value is not None:
            properties["sensor_value"] = self.sensor_value
        if self.type_ is not None:
            properties["type"] = self.type_
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class EquipmentModuleWithStartEndTime(EquipmentModuleApply, StartEndTimeApply):
    ...


class EquipmentModuleList(TypeList[EquipmentModule]):
    """List of equipment modules in read version."""

    _NODE = EquipmentModule

    def as_apply(self) -> EquipmentModuleApplyList:
        """Convert this read version of equipment module to a write version."""
        return EquipmentModuleApplyList([node.as_apply() for node in self.data])


class EquipmentModuleApplyList(TypeApplyList[EquipmentModuleApply]):
    """List of equipment modules in write version."""

    _NODE = EquipmentModuleApply


module = EquipmentModuleWithStartEndTime(
    space="IntegrationTestsImmutable",
    external_id="EquipmentModule",
    name="EquipmentModule",
    type_="EquipmentModule",
    start_time=datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
    end_time=datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
)
