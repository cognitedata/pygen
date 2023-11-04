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
    """This represent a read version of available trajectory station property.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the available trajectory station property.
        name: The name field.
        station_property_unit_id: The station property unit id field.
        trajectory_station_property_type_id: The trajectory station property type id field.
        created_time: The created time of the available trajectory station property node.
        last_updated_time: The last updated time of the available trajectory station property node.
        deleted_time: If present, the deleted time of the available trajectory station property node.
        version: The version of the available trajectory station property node.
    """

    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = Field(None, alias="Name")
    station_property_unit_id: Optional[str] = Field(None, alias="StationPropertyUnitID")
    trajectory_station_property_type_id: Optional[str] = Field(None, alias="TrajectoryStationPropertyTypeID")

    def as_apply(self) -> AvailableTrajectoryStationPropertiesApply:
        """Convert this read version of available trajectory station property to a write version."""
        return AvailableTrajectoryStationPropertiesApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            station_property_unit_id=self.station_property_unit_id,
            trajectory_station_property_type_id=self.trajectory_station_property_type_id,
        )


class AvailableTrajectoryStationPropertiesApply(DomainModelApply):
    """This represent a write version of available trajectory station property.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the available trajectory station property.
        name: The name field.
        station_property_unit_id: The station property unit id field.
        trajectory_station_property_type_id: The trajectory station property type id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = Field(None, alias="Name")
    station_property_unit_id: Optional[str] = Field(None, alias="StationPropertyUnitID")
    trajectory_station_property_type_id: Optional[str] = Field(None, alias="TrajectoryStationPropertyTypeID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["Name"] = self.name
        if self.station_property_unit_id is not None:
            properties["StationPropertyUnitID"] = self.station_property_unit_id
        if self.trajectory_station_property_type_id is not None:
            properties["TrajectoryStationPropertyTypeID"] = self.trajectory_station_property_type_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view
                or dm.ViewId("IntegrationTestsImmutable", "AvailableTrajectoryStationProperties", "e1c516b799081a"),
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


class AvailableTrajectoryStationPropertiesList(TypeList[AvailableTrajectoryStationProperties]):
    """List of available trajectory station properties in read version."""

    _NODE = AvailableTrajectoryStationProperties

    def as_apply(self) -> AvailableTrajectoryStationPropertiesApplyList:
        """Convert this read version of available trajectory station property to a write version."""
        return AvailableTrajectoryStationPropertiesApplyList([node.as_apply() for node in self.data])


class AvailableTrajectoryStationPropertiesApplyList(TypeApplyList[AvailableTrajectoryStationPropertiesApply]):
    """List of available trajectory station properties in write version."""

    _NODE = AvailableTrajectoryStationPropertiesApply
