from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
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
        data_record: The data record of the main shaft node.
        bending_x: The bending x field.
        bending_y: The bending y field.
        calculated_tilt_moment: The calculated tilt moment field.
        calculated_yaw_moment: The calculated yaw moment field.
        torque: The torque field.
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
            data_record=DataRecordWrite(existing_version=self.data_record.version),
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
        data_record: The data record of the main shaft node.
        bending_x: The bending x field.
        bending_y: The bending y field.
        calculated_tilt_moment: The calculated tilt moment field.
        calculated_yaw_moment: The calculated yaw moment field.
        torque: The torque field.
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
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(MainShaft, dm.ViewId("power-models", "MainShaft", "1"))

        properties: dict[str, Any] = {}

        if self.bending_x is not None or write_none:
            if isinstance(self.bending_x, str) or self.bending_x is None:
                properties["bending_x"] = self.bending_x
            else:
                properties["bending_x"] = self.bending_x.external_id

        if self.bending_y is not None or write_none:
            if isinstance(self.bending_y, str) or self.bending_y is None:
                properties["bending_y"] = self.bending_y
            else:
                properties["bending_y"] = self.bending_y.external_id

        if self.calculated_tilt_moment is not None or write_none:
            if isinstance(self.calculated_tilt_moment, str) or self.calculated_tilt_moment is None:
                properties["calculated_tilt_moment"] = self.calculated_tilt_moment
            else:
                properties["calculated_tilt_moment"] = self.calculated_tilt_moment.external_id

        if self.calculated_yaw_moment is not None or write_none:
            if isinstance(self.calculated_yaw_moment, str) or self.calculated_yaw_moment is None:
                properties["calculated_yaw_moment"] = self.calculated_yaw_moment
            else:
                properties["calculated_yaw_moment"] = self.calculated_yaw_moment.external_id

        if self.torque is not None or write_none:
            if isinstance(self.torque, str) or self.torque is None:
                properties["torque"] = self.torque
            else:
                properties["torque"] = self.torque.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None