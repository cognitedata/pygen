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
    """This represents the reading version of available trajectory station property.

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

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = Field(None, alias="Name")
    station_property_unit_id: Optional[str] = Field(None, alias="StationPropertyUnitID")
    trajectory_station_property_type_id: Optional[str] = Field(None, alias="TrajectoryStationPropertyTypeID")

    def as_apply(self) -> AvailableTrajectoryStationPropertiesApply:
        """Convert this read version of available trajectory station property to the writing version."""
        return AvailableTrajectoryStationPropertiesApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            station_property_unit_id=self.station_property_unit_id,
            trajectory_station_property_type_id=self.trajectory_station_property_type_id,
        )


class AvailableTrajectoryStationPropertiesApply(DomainModelApply):
    """This represents the writing version of available trajectory station property.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the available trajectory station property.
        name: The name field.
        station_property_unit_id: The station property unit id field.
        trajectory_station_property_type_id: The trajectory station property type id field.
        existing_version: Fail the ingestion request if the available trajectory station property version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = Field(None, alias="Name")
    station_property_unit_id: Optional[str] = Field(None, alias="StationPropertyUnitID")
    trajectory_station_property_type_id: Optional[str] = Field(None, alias="TrajectoryStationPropertyTypeID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "AvailableTrajectoryStationProperties", "e1c516b799081a"
        )

        properties = {}
        if self.name is not None:
            properties["Name"] = self.name
        if self.station_property_unit_id is not None:
            properties["StationPropertyUnitID"] = self.station_property_unit_id
        if self.trajectory_station_property_type_id is not None:
            properties["TrajectoryStationPropertyTypeID"] = self.trajectory_station_property_type_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "AvailableTrajectoryStationProperties"),
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


class AvailableTrajectoryStationPropertiesList(DomainModelList[AvailableTrajectoryStationProperties]):
    """List of available trajectory station properties in the read version."""

    _INSTANCE = AvailableTrajectoryStationProperties

    def as_apply(self) -> AvailableTrajectoryStationPropertiesApplyList:
        """Convert these read versions of available trajectory station property to the writing versions."""
        return AvailableTrajectoryStationPropertiesApplyList([node.as_apply() for node in self.data])


class AvailableTrajectoryStationPropertiesApplyList(DomainModelApplyList[AvailableTrajectoryStationPropertiesApply]):
    """List of available trajectory station properties in the writing version."""

    _INSTANCE = AvailableTrajectoryStationPropertiesApply


def _create_available_trajectory_station_property_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    station_property_unit_id: str | list[str] | None = None,
    station_property_unit_id_prefix: str | None = None,
    trajectory_station_property_type_id: str | list[str] | None = None,
    trajectory_station_property_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Name"), value=name_prefix))
    if station_property_unit_id is not None and isinstance(station_property_unit_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("StationPropertyUnitID"), value=station_property_unit_id)
        )
    if station_property_unit_id and isinstance(station_property_unit_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("StationPropertyUnitID"), values=station_property_unit_id))
    if station_property_unit_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("StationPropertyUnitID"), value=station_property_unit_id_prefix)
        )
    if trajectory_station_property_type_id is not None and isinstance(trajectory_station_property_type_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("TrajectoryStationPropertyTypeID"), value=trajectory_station_property_type_id
            )
        )
    if trajectory_station_property_type_id and isinstance(trajectory_station_property_type_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("TrajectoryStationPropertyTypeID"), values=trajectory_station_property_type_id
            )
        )
    if trajectory_station_property_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("TrajectoryStationPropertyTypeID"),
                value=trajectory_station_property_type_id_prefix,
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
