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


__all__ = [
    "HighSpeedShaft",
    "HighSpeedShaftApply",
    "HighSpeedShaftList",
    "HighSpeedShaftApplyList",
    "HighSpeedShaftFields",
    "HighSpeedShaftTextFields",
]


HighSpeedShaftTextFields = Literal["bending_moment_y", "bending_monent_x", "torque"]
HighSpeedShaftFields = Literal["bending_moment_y", "bending_monent_x", "torque"]

_HIGHSPEEDSHAFT_PROPERTIES_BY_FIELD = {
    "bending_moment_y": "bending_moment_y",
    "bending_monent_x": "bending_monent_x",
    "torque": "torque",
}


class HighSpeedShaft(DomainModel):
    """This represents the reading version of high speed shaft.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the high speed shaft.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        torque: The torque field.
        created_time: The created time of the high speed shaft node.
        last_updated_time: The last updated time of the high speed shaft node.
        deleted_time: If present, the deleted time of the high speed shaft node.
        version: The version of the high speed shaft node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bending_moment_y: Union[TimeSeries, str, None] = None
    bending_monent_x: Union[TimeSeries, str, None] = None
    torque: Union[TimeSeries, str, None] = None

    def as_apply(self) -> HighSpeedShaftApply:
        """Convert this read version of high speed shaft to the writing version."""
        return HighSpeedShaftApply(
            space=self.space,
            external_id=self.external_id,
            bending_moment_y=self.bending_moment_y,
            bending_monent_x=self.bending_monent_x,
            torque=self.torque,
        )


class HighSpeedShaftApply(DomainModelApply):
    """This represents the writing version of high speed shaft.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the high speed shaft.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        torque: The torque field.
        existing_version: Fail the ingestion request if the high speed shaft version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bending_moment_y: Union[TimeSeries, str, None] = None
    bending_monent_x: Union[TimeSeries, str, None] = None
    torque: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-models", "HighSpeedShaft", "1"
        )

        properties = {}

        if self.bending_moment_y is not None:
            properties["bending_moment_y"] = (
                self.bending_moment_y if isinstance(self.bending_moment_y, str) else self.bending_moment_y.external_id
            )

        if self.bending_monent_x is not None:
            properties["bending_monent_x"] = (
                self.bending_monent_x if isinstance(self.bending_monent_x, str) else self.bending_monent_x.external_id
            )

        if self.torque is not None:
            properties["torque"] = self.torque if isinstance(self.torque, str) else self.torque.external_id

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

        if isinstance(self.bending_moment_y, CogniteTimeSeries):
            resources.time_series.append(self.bending_moment_y)

        if isinstance(self.bending_monent_x, CogniteTimeSeries):
            resources.time_series.append(self.bending_monent_x)

        if isinstance(self.torque, CogniteTimeSeries):
            resources.time_series.append(self.torque)

        return resources


class HighSpeedShaftList(DomainModelList[HighSpeedShaft]):
    """List of high speed shafts in the read version."""

    _INSTANCE = HighSpeedShaft

    def as_apply(self) -> HighSpeedShaftApplyList:
        """Convert these read versions of high speed shaft to the writing versions."""
        return HighSpeedShaftApplyList([node.as_apply() for node in self.data])


class HighSpeedShaftApplyList(DomainModelApplyList[HighSpeedShaftApply]):
    """List of high speed shafts in the writing version."""

    _INSTANCE = HighSpeedShaftApply


def _create_high_speed_shaft_filter(
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
