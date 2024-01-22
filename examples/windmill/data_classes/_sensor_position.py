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
    "SensorPosition",
    "SensorPositionApply",
    "SensorPositionList",
    "SensorPositionApplyList",
    "SensorPositionFields",
    "SensorPositionTextFields",
]


SensorPositionTextFields = Literal[
    "edgewise_bend_mom_crosstalk_corrected",
    "edgewise_bend_mom_offset",
    "edgewise_bend_mom_offset_crosstalk_corrected",
    "edgewisewise_bend_mom",
    "flapwise_bend_mom",
    "flapwise_bend_mom_crosstalk_corrected",
    "flapwise_bend_mom_offset",
    "flapwise_bend_mom_offset_crosstalk_corrected",
]
SensorPositionFields = Literal[
    "edgewise_bend_mom_crosstalk_corrected",
    "edgewise_bend_mom_offset",
    "edgewise_bend_mom_offset_crosstalk_corrected",
    "edgewisewise_bend_mom",
    "flapwise_bend_mom",
    "flapwise_bend_mom_crosstalk_corrected",
    "flapwise_bend_mom_offset",
    "flapwise_bend_mom_offset_crosstalk_corrected",
    "position",
]

_SENSORPOSITION_PROPERTIES_BY_FIELD = {
    "edgewise_bend_mom_crosstalk_corrected": "edgewise_bend_mom_crosstalk_corrected",
    "edgewise_bend_mom_offset": "edgewise_bend_mom_offset",
    "edgewise_bend_mom_offset_crosstalk_corrected": "edgewise_bend_mom_offset_crosstalk_corrected",
    "edgewisewise_bend_mom": "edgewisewise_bend_mom",
    "flapwise_bend_mom": "flapwise_bend_mom",
    "flapwise_bend_mom_crosstalk_corrected": "flapwise_bend_mom_crosstalk_corrected",
    "flapwise_bend_mom_offset": "flapwise_bend_mom_offset",
    "flapwise_bend_mom_offset_crosstalk_corrected": "flapwise_bend_mom_offset_crosstalk_corrected",
    "position": "position",
}


class SensorPosition(DomainModel):
    """This represents the reading version of sensor position.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sensor position.
        data_record: The data record of the sensor position node.
        edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected field.
        edgewise_bend_mom_offset: The edgewise bend mom offset field.
        edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected field.
        edgewisewise_bend_mom: The edgewisewise bend mom field.
        flapwise_bend_mom: The flapwise bend mom field.
        flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected field.
        flapwise_bend_mom_offset: The flapwise bend mom offset field.
        flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected field.
        position: The position field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    edgewise_bend_mom_crosstalk_corrected: Union[TimeSeries, str, None] = None
    edgewise_bend_mom_offset: Union[TimeSeries, str, None] = None
    edgewise_bend_mom_offset_crosstalk_corrected: Union[TimeSeries, str, None] = None
    edgewisewise_bend_mom: Union[TimeSeries, str, None] = None
    flapwise_bend_mom: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_crosstalk_corrected: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_offset: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_offset_crosstalk_corrected: Union[TimeSeries, str, None] = None
    position: Optional[float] = None

    def as_apply(self) -> SensorPositionApply:
        """Convert this read version of sensor position to the writing version."""
        return SensorPositionApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            edgewise_bend_mom_crosstalk_corrected=self.edgewise_bend_mom_crosstalk_corrected,
            edgewise_bend_mom_offset=self.edgewise_bend_mom_offset,
            edgewise_bend_mom_offset_crosstalk_corrected=self.edgewise_bend_mom_offset_crosstalk_corrected,
            edgewisewise_bend_mom=self.edgewisewise_bend_mom,
            flapwise_bend_mom=self.flapwise_bend_mom,
            flapwise_bend_mom_crosstalk_corrected=self.flapwise_bend_mom_crosstalk_corrected,
            flapwise_bend_mom_offset=self.flapwise_bend_mom_offset,
            flapwise_bend_mom_offset_crosstalk_corrected=self.flapwise_bend_mom_offset_crosstalk_corrected,
            position=self.position,
        )


class SensorPositionApply(DomainModelApply):
    """This represents the writing version of sensor position.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sensor position.
        data_record: The data record of the sensor position node.
        edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected field.
        edgewise_bend_mom_offset: The edgewise bend mom offset field.
        edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected field.
        edgewisewise_bend_mom: The edgewisewise bend mom field.
        flapwise_bend_mom: The flapwise bend mom field.
        flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected field.
        flapwise_bend_mom_offset: The flapwise bend mom offset field.
        flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected field.
        position: The position field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    edgewise_bend_mom_crosstalk_corrected: Union[TimeSeries, str, None] = None
    edgewise_bend_mom_offset: Union[TimeSeries, str, None] = None
    edgewise_bend_mom_offset_crosstalk_corrected: Union[TimeSeries, str, None] = None
    edgewisewise_bend_mom: Union[TimeSeries, str, None] = None
    flapwise_bend_mom: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_crosstalk_corrected: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_offset: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_offset_crosstalk_corrected: Union[TimeSeries, str, None] = None
    position: Optional[float] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(SensorPosition, dm.ViewId("power-models", "SensorPosition", "1"))

        properties: dict[str, Any] = {}

        if self.edgewise_bend_mom_crosstalk_corrected is not None or write_none:
            if (
                isinstance(self.edgewise_bend_mom_crosstalk_corrected, str)
                or self.edgewise_bend_mom_crosstalk_corrected is None
            ):
                properties["edgewise_bend_mom_crosstalk_corrected"] = self.edgewise_bend_mom_crosstalk_corrected
            else:
                properties[
                    "edgewise_bend_mom_crosstalk_corrected"
                ] = self.edgewise_bend_mom_crosstalk_corrected.external_id

        if self.edgewise_bend_mom_offset is not None or write_none:
            if isinstance(self.edgewise_bend_mom_offset, str) or self.edgewise_bend_mom_offset is None:
                properties["edgewise_bend_mom_offset"] = self.edgewise_bend_mom_offset
            else:
                properties["edgewise_bend_mom_offset"] = self.edgewise_bend_mom_offset.external_id

        if self.edgewise_bend_mom_offset_crosstalk_corrected is not None or write_none:
            if (
                isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, str)
                or self.edgewise_bend_mom_offset_crosstalk_corrected is None
            ):
                properties[
                    "edgewise_bend_mom_offset_crosstalk_corrected"
                ] = self.edgewise_bend_mom_offset_crosstalk_corrected
            else:
                properties[
                    "edgewise_bend_mom_offset_crosstalk_corrected"
                ] = self.edgewise_bend_mom_offset_crosstalk_corrected.external_id

        if self.edgewisewise_bend_mom is not None or write_none:
            if isinstance(self.edgewisewise_bend_mom, str) or self.edgewisewise_bend_mom is None:
                properties["edgewisewise_bend_mom"] = self.edgewisewise_bend_mom
            else:
                properties["edgewisewise_bend_mom"] = self.edgewisewise_bend_mom.external_id

        if self.flapwise_bend_mom is not None or write_none:
            if isinstance(self.flapwise_bend_mom, str) or self.flapwise_bend_mom is None:
                properties["flapwise_bend_mom"] = self.flapwise_bend_mom
            else:
                properties["flapwise_bend_mom"] = self.flapwise_bend_mom.external_id

        if self.flapwise_bend_mom_crosstalk_corrected is not None or write_none:
            if (
                isinstance(self.flapwise_bend_mom_crosstalk_corrected, str)
                or self.flapwise_bend_mom_crosstalk_corrected is None
            ):
                properties["flapwise_bend_mom_crosstalk_corrected"] = self.flapwise_bend_mom_crosstalk_corrected
            else:
                properties[
                    "flapwise_bend_mom_crosstalk_corrected"
                ] = self.flapwise_bend_mom_crosstalk_corrected.external_id

        if self.flapwise_bend_mom_offset is not None or write_none:
            if isinstance(self.flapwise_bend_mom_offset, str) or self.flapwise_bend_mom_offset is None:
                properties["flapwise_bend_mom_offset"] = self.flapwise_bend_mom_offset
            else:
                properties["flapwise_bend_mom_offset"] = self.flapwise_bend_mom_offset.external_id

        if self.flapwise_bend_mom_offset_crosstalk_corrected is not None or write_none:
            if (
                isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, str)
                or self.flapwise_bend_mom_offset_crosstalk_corrected is None
            ):
                properties[
                    "flapwise_bend_mom_offset_crosstalk_corrected"
                ] = self.flapwise_bend_mom_offset_crosstalk_corrected
            else:
                properties[
                    "flapwise_bend_mom_offset_crosstalk_corrected"
                ] = self.flapwise_bend_mom_offset_crosstalk_corrected.external_id

        if self.position is not None or write_none:
            properties["position"] = self.position

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

        if isinstance(self.edgewise_bend_mom_crosstalk_corrected, CogniteTimeSeries):
            resources.time_series.append(self.edgewise_bend_mom_crosstalk_corrected)

        if isinstance(self.edgewise_bend_mom_offset, CogniteTimeSeries):
            resources.time_series.append(self.edgewise_bend_mom_offset)

        if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, CogniteTimeSeries):
            resources.time_series.append(self.edgewise_bend_mom_offset_crosstalk_corrected)

        if isinstance(self.edgewisewise_bend_mom, CogniteTimeSeries):
            resources.time_series.append(self.edgewisewise_bend_mom)

        if isinstance(self.flapwise_bend_mom, CogniteTimeSeries):
            resources.time_series.append(self.flapwise_bend_mom)

        if isinstance(self.flapwise_bend_mom_crosstalk_corrected, CogniteTimeSeries):
            resources.time_series.append(self.flapwise_bend_mom_crosstalk_corrected)

        if isinstance(self.flapwise_bend_mom_offset, CogniteTimeSeries):
            resources.time_series.append(self.flapwise_bend_mom_offset)

        if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, CogniteTimeSeries):
            resources.time_series.append(self.flapwise_bend_mom_offset_crosstalk_corrected)

        return resources


class SensorPositionList(DomainModelList[SensorPosition]):
    """List of sensor positions in the read version."""

    _INSTANCE = SensorPosition

    def as_apply(self) -> SensorPositionApplyList:
        """Convert these read versions of sensor position to the writing versions."""
        return SensorPositionApplyList([node.as_apply() for node in self.data])


class SensorPositionApplyList(DomainModelApplyList[SensorPositionApply]):
    """List of sensor positions in the writing version."""

    _INSTANCE = SensorPositionApply


def _create_sensor_position_filter(
    view_id: dm.ViewId,
    min_position: float | None = None,
    max_position: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_position or max_position:
        filters.append(dm.filters.Range(view_id.as_property_ref("position"), gte=min_position, lte=max_position))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
