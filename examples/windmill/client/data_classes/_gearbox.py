from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

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


__all__ = ["Gearbox", "GearboxApply", "GearboxList", "GearboxApplyList", "GearboxFields", "GearboxTextFields"]


GearboxTextFields = Literal["displacement_x", "displacement_y", "displacement_z"]
GearboxFields = Literal["displacement_x", "displacement_y", "displacement_z"]

_GEARBOX_PROPERTIES_BY_FIELD = {
    "displacement_x": "displacement_x",
    "displacement_y": "displacement_y",
    "displacement_z": "displacement_z",
}


class Gearbox(DomainModel):
    """This represents the reading version of gearbox.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
        created_time: The created time of the gearbox node.
        last_updated_time: The last updated time of the gearbox node.
        deleted_time: If present, the deleted time of the gearbox node.
        version: The version of the gearbox node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    displacement_x: Union[TimeSeries, str, None] = None
    displacement_y: Union[TimeSeries, str, None] = None
    displacement_z: Union[TimeSeries, str, None] = None

    def as_apply(self) -> GearboxApply:
        """Convert this read version of gearbox to the writing version."""
        return GearboxApply(
            space=self.space,
            external_id=self.external_id,
            displacement_x=self.displacement_x,
            displacement_y=self.displacement_y,
            displacement_z=self.displacement_z,
        )


class GearboxApply(DomainModelApply):
    """This represents the writing version of gearbox.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
        existing_version: Fail the ingestion request if the gearbox version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    displacement_x: Union[TimeSeries, str, None] = None
    displacement_y: Union[TimeSeries, str, None] = None
    displacement_z: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Gearbox, dm.ViewId("power-models", "Gearbox", "1"))

        properties = {}

        if self.displacement_x is not None:
            properties["displacement_x"] = (
                self.displacement_x if isinstance(self.displacement_x, str) else self.displacement_x.external_id
            )

        if self.displacement_y is not None:
            properties["displacement_y"] = (
                self.displacement_y if isinstance(self.displacement_y, str) else self.displacement_y.external_id
            )

        if self.displacement_z is not None:
            properties["displacement_z"] = (
                self.displacement_z if isinstance(self.displacement_z, str) else self.displacement_z.external_id
            )

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

        if isinstance(self.displacement_x, CogniteTimeSeries):
            resources.time_series.append(self.displacement_x)

        if isinstance(self.displacement_y, CogniteTimeSeries):
            resources.time_series.append(self.displacement_y)

        if isinstance(self.displacement_z, CogniteTimeSeries):
            resources.time_series.append(self.displacement_z)

        return resources


class GearboxList(DomainModelList[Gearbox]):
    """List of gearboxes in the read version."""

    _INSTANCE = Gearbox

    def as_apply(self) -> GearboxApplyList:
        """Convert these read versions of gearbox to the writing versions."""
        return GearboxApplyList([node.as_apply() for node in self.data])


class GearboxApplyList(DomainModelApplyList[GearboxApply]):
    """List of gearboxes in the writing version."""

    _INSTANCE = GearboxApply


def _create_gearbox_filter(
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
