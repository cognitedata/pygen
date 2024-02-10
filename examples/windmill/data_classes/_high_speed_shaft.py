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
        data_record: The data record of the high speed shaft node.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        torque: The torque field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    bending_moment_y: Union[TimeSeries, str, None] = None
    bending_monent_x: Union[TimeSeries, str, None] = None
    torque: Union[TimeSeries, str, None] = None

    def as_apply(self) -> HighSpeedShaftApply:
        """Convert this read version of high speed shaft to the writing version."""
        return HighSpeedShaftApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
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
        data_record: The data record of the high speed shaft node.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        torque: The torque field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    bending_moment_y: Union[TimeSeries, str, None] = None
    bending_monent_x: Union[TimeSeries, str, None] = None
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

        write_view = (view_by_read_class or {}).get(HighSpeedShaft, dm.ViewId("power-models", "HighSpeedShaft", "1"))

        properties: dict[str, Any] = {}

        if self.bending_moment_y is not None or write_none:
            if isinstance(self.bending_moment_y, str) or self.bending_moment_y is None:
                properties["bending_moment_y"] = self.bending_moment_y
            else:
                properties["bending_moment_y"] = self.bending_moment_y.external_id

        if self.bending_monent_x is not None or write_none:
            if isinstance(self.bending_monent_x, str) or self.bending_monent_x is None:
                properties["bending_monent_x"] = self.bending_monent_x
            else:
                properties["bending_monent_x"] = self.bending_monent_x.external_id

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
        return HighSpeedShaftApplyList([node.as_write() for node in self.data])


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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
