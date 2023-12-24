from __future__ import annotations

from typing import Literal, Optional, Union  # noqa: F401

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = ["Rotor", "RotorApply", "RotorList", "RotorApplyList", "RotorFields", "RotorTextFields"]


RotorTextFields = Literal["rotor_speed_controller", "rpm_low_speed_shaft"]
RotorFields = Literal["rotor_speed_controller", "rpm_low_speed_shaft"]

_ROTOR_PROPERTIES_BY_FIELD = {
    "rotor_speed_controller": "rotor_speed_controller",
    "rpm_low_speed_shaft": "rpm_low_speed_shaft",
}


class Rotor(DomainModel):
    """This represents the reading version of rotor.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
        created_time: The created time of the rotor node.
        last_updated_time: The last updated time of the rotor node.
        deleted_time: If present, the deleted time of the rotor node.
        version: The version of the rotor node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    rotor_speed_controller: Union[TimeSeries, str, None] = None
    rpm_low_speed_shaft: Union[TimeSeries, str, None] = None

    def as_apply(self) -> RotorApply:
        """Convert this read version of rotor to the writing version."""
        return RotorApply(
            space=self.space,
            external_id=self.external_id,
            rotor_speed_controller=self.rotor_speed_controller,
            rpm_low_speed_shaft=self.rpm_low_speed_shaft,
        )


class RotorApply(DomainModelApply):
    """This represents the writing version of rotor.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
        existing_version: Fail the ingestion request if the rotor version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    rotor_speed_controller: Union[TimeSeries, str, None] = None
    rpm_low_speed_shaft: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-models", "Rotor", "1"
        )

        properties = {}

        if self.rotor_speed_controller is not None:
            properties["rotor_speed_controller"] = (
                self.rotor_speed_controller
                if isinstance(self.rotor_speed_controller, str)
                else self.rotor_speed_controller.external_id
            )

        if self.rpm_low_speed_shaft is not None:
            properties["rpm_low_speed_shaft"] = (
                self.rpm_low_speed_shaft
                if isinstance(self.rpm_low_speed_shaft, str)
                else self.rpm_low_speed_shaft.external_id
            )

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

        if isinstance(self.rotor_speed_controller, CogniteTimeSeries):
            resources.time_series.append(self.rotor_speed_controller)

        if isinstance(self.rpm_low_speed_shaft, CogniteTimeSeries):
            resources.time_series.append(self.rpm_low_speed_shaft)

        return resources


class RotorList(DomainModelList[Rotor]):
    """List of rotors in the read version."""

    _INSTANCE = Rotor

    def as_apply(self) -> RotorApplyList:
        """Convert these read versions of rotor to the writing versions."""
        return RotorApplyList([node.as_apply() for node in self.data])


class RotorApplyList(DomainModelApplyList[RotorApply]):
    """List of rotors in the writing version."""

    _INSTANCE = RotorApply


def _create_rotor_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
