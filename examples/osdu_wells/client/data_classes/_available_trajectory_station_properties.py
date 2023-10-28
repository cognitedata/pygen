from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "AvailableTrajectoryStationProperties",
    "AvailableTrajectoryStationPropertiesApply",
    "AvailableTrajectoryStationPropertiesList",
    "AvailableTrajectoryStationPropertiesApplyList",
    "AvailableTrajectoryStationPropertiesFields",
    "AvailableTrajectoryStationPropertiesTextFields",
]


AvailableTrajectoryStationPropertiesTextFields = Literal[
    "name", "station_property_unit_id", "trajectory_station_property_type_id"
]
AvailableTrajectoryStationPropertiesFields = Literal[
    "name", "station_property_unit_id", "trajectory_station_property_type_id"
]

_AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD = {
    "name": "Name",
    "station_property_unit_id": "StationPropertyUnitID",
    "trajectory_station_property_type_id": "TrajectoryStationPropertyTypeID",
}


class AvailableTrajectoryStationProperties(DomainModel):
    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = Field(None, alias="Name")
    station_property_unit_id: Optional[str] = Field(None, alias="StationPropertyUnitID")
    trajectory_station_property_type_id: Optional[str] = Field(None, alias="TrajectoryStationPropertyTypeID")

    def as_apply(self) -> AvailableTrajectoryStationPropertiesApply:
        return AvailableTrajectoryStationPropertiesApply(
            external_id=self.external_id,
            name=self.name,
            station_property_unit_id=self.station_property_unit_id,
            trajectory_station_property_type_id=self.trajectory_station_property_type_id,
        )


class AvailableTrajectoryStationPropertiesApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = Field(None, alias="Name")
    station_property_unit_id: Optional[str] = Field(None, alias="StationPropertyUnitID")
    trajectory_station_property_type_id: Optional[str] = Field(None, alias="TrajectoryStationPropertyTypeID")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["Name"] = self.name
        if self.station_property_unit_id is not None:
            properties["StationPropertyUnitID"] = self.station_property_unit_id
        if self.trajectory_station_property_type_id is not None:
            properties["TrajectoryStationPropertyTypeID"] = self.trajectory_station_property_type_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "AvailableTrajectoryStationProperties"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class AvailableTrajectoryStationPropertiesList(TypeList[AvailableTrajectoryStationProperties]):
    _NODE = AvailableTrajectoryStationProperties

    def as_apply(self) -> AvailableTrajectoryStationPropertiesApplyList:
        return AvailableTrajectoryStationPropertiesApplyList([node.as_apply() for node in self.data])


class AvailableTrajectoryStationPropertiesApplyList(TypeApplyList[AvailableTrajectoryStationPropertiesApply]):
    _NODE = AvailableTrajectoryStationPropertiesApply
