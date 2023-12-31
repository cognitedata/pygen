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


__all__ = [
    "MainShaft",
    "MainShaftApply",
    "MainShaftList",
    "MainShaftApplyList",
    "MainShaftFields",
    "MainShaftTextFields",
]


MainShaftTextFields = Literal["bending_x", "bending_y", "calculated_tilt_moment", "calculated_yaw_moment", "torque"]
MainShaftFields = Literal["bending_x", "bending_y", "calculated_tilt_moment", "calculated_yaw_moment", "torque"]

_MAINSHAFT_PROPERTIES_BY_FIELD = {
    "bending_x": "bending_x",
    "bending_y": "bending_y",
    "calculated_tilt_moment": "calculated_tilt_moment",
    "calculated_yaw_moment": "calculated_yaw_moment",
    "torque": "torque",
}


class MainShaft(DomainModel):
    """This represents the reading version of main shaft.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main shaft.
        bending_x: The bending x field.
        bending_y: The bending y field.
        calculated_tilt_moment: The calculated tilt moment field.
        calculated_yaw_moment: The calculated yaw moment field.
        torque: The torque field.
        created_time: The created time of the main shaft node.
        last_updated_time: The last updated time of the main shaft node.
        deleted_time: If present, the deleted time of the main shaft node.
        version: The version of the main shaft node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    bending_x: Union[TimeSeries, str, None] = None
    bending_y: Union[TimeSeries, str, None] = None
    calculated_tilt_moment: Union[TimeSeries, str, None] = None
    calculated_yaw_moment: Union[TimeSeries, str, None] = None
    torque: Union[TimeSeries, str, None] = None

    def as_apply(self) -> MainShaftApply:
        """Convert this read version of main shaft to the writing version."""
        return MainShaftApply(
            space=self.space,
            external_id=self.external_id,
            bending_x=self.bending_x,
            bending_y=self.bending_y,
            calculated_tilt_moment=self.calculated_tilt_moment,
            calculated_yaw_moment=self.calculated_yaw_moment,
            torque=self.torque,
        )


class MainShaftApply(DomainModelApply):
    """This represents the writing version of main shaft.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main shaft.
        bending_x: The bending x field.
        bending_y: The bending y field.
        calculated_tilt_moment: The calculated tilt moment field.
        calculated_yaw_moment: The calculated yaw moment field.
        torque: The torque field.
        existing_version: Fail the ingestion request if the main shaft version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    bending_x: Union[TimeSeries, str, None] = None
    bending_y: Union[TimeSeries, str, None] = None
    calculated_tilt_moment: Union[TimeSeries, str, None] = None
    calculated_yaw_moment: Union[TimeSeries, str, None] = None
    torque: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(MainShaft, dm.ViewId("power-models", "MainShaft", "1"))

        properties = {}

        if self.bending_x is not None:
            properties["bending_x"] = self.bending_x if isinstance(self.bending_x, str) else self.bending_x.external_id

        if self.bending_y is not None:
            properties["bending_y"] = self.bending_y if isinstance(self.bending_y, str) else self.bending_y.external_id

        if self.calculated_tilt_moment is not None:
            properties["calculated_tilt_moment"] = (
                self.calculated_tilt_moment
                if isinstance(self.calculated_tilt_moment, str)
                else self.calculated_tilt_moment.external_id
            )

        if self.calculated_yaw_moment is not None:
            properties["calculated_yaw_moment"] = (
                self.calculated_yaw_moment
                if isinstance(self.calculated_yaw_moment, str)
                else self.calculated_yaw_moment.external_id
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

        if isinstance(self.bending_x, CogniteTimeSeries):
            resources.time_series.append(self.bending_x)

        if isinstance(self.bending_y, CogniteTimeSeries):
            resources.time_series.append(self.bending_y)

        if isinstance(self.calculated_tilt_moment, CogniteTimeSeries):
            resources.time_series.append(self.calculated_tilt_moment)

        if isinstance(self.calculated_yaw_moment, CogniteTimeSeries):
            resources.time_series.append(self.calculated_yaw_moment)

        if isinstance(self.torque, CogniteTimeSeries):
            resources.time_series.append(self.torque)

        return resources


class MainShaftList(DomainModelList[MainShaft]):
    """List of main shafts in the read version."""

    _INSTANCE = MainShaft

    def as_apply(self) -> MainShaftApplyList:
        """Convert these read versions of main shaft to the writing versions."""
        return MainShaftApplyList([node.as_apply() for node in self.data])


class MainShaftApplyList(DomainModelApplyList[MainShaftApply]):
    """List of main shafts in the writing version."""

    _INSTANCE = MainShaftApply


def _create_main_shaft_filter(
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
