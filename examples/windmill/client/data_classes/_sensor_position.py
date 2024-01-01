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
        edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected field.
        edgewise_bend_mom_offset: The edgewise bend mom offset field.
        edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected field.
        edgewisewise_bend_mom: The edgewisewise bend mom field.
        flapwise_bend_mom: The flapwise bend mom field.
        flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected field.
        flapwise_bend_mom_offset: The flapwise bend mom offset field.
        flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected field.
        position: The position field.
        created_time: The created time of the sensor position node.
        last_updated_time: The last updated time of the sensor position node.
        deleted_time: If present, the deleted time of the sensor position node.
        version: The version of the sensor position node.
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
        edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected field.
        edgewise_bend_mom_offset: The edgewise bend mom offset field.
        edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected field.
        edgewisewise_bend_mom: The edgewisewise bend mom field.
        flapwise_bend_mom: The flapwise bend mom field.
        flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected field.
        flapwise_bend_mom_offset: The flapwise bend mom offset field.
        flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected field.
        position: The position field.
        existing_version: Fail the ingestion request if the sensor position version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
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

        properties = {}

        if self.edgewise_bend_mom_crosstalk_corrected is not None or write_none:
            properties["edgewise_bend_mom_crosstalk_corrected"] = (
                self.edgewise_bend_mom_crosstalk_corrected
                if isinstance(self.edgewise_bend_mom_crosstalk_corrected, str)
                else self.edgewise_bend_mom_crosstalk_corrected.external_id
            )

        if self.edgewise_bend_mom_offset is not None or write_none:
            properties["edgewise_bend_mom_offset"] = (
                self.edgewise_bend_mom_offset
                if isinstance(self.edgewise_bend_mom_offset, str)
                else self.edgewise_bend_mom_offset.external_id
            )

        if self.edgewise_bend_mom_offset_crosstalk_corrected is not None or write_none:
            properties["edgewise_bend_mom_offset_crosstalk_corrected"] = (
                self.edgewise_bend_mom_offset_crosstalk_corrected
                if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, str)
                else self.edgewise_bend_mom_offset_crosstalk_corrected.external_id
            )

        if self.edgewisewise_bend_mom is not None or write_none:
            properties["edgewisewise_bend_mom"] = (
                self.edgewisewise_bend_mom
                if isinstance(self.edgewisewise_bend_mom, str)
                else self.edgewisewise_bend_mom.external_id
            )

        if self.flapwise_bend_mom is not None or write_none:
            properties["flapwise_bend_mom"] = (
                self.flapwise_bend_mom
                if isinstance(self.flapwise_bend_mom, str)
                else self.flapwise_bend_mom.external_id
            )

        if self.flapwise_bend_mom_crosstalk_corrected is not None or write_none:
            properties["flapwise_bend_mom_crosstalk_corrected"] = (
                self.flapwise_bend_mom_crosstalk_corrected
                if isinstance(self.flapwise_bend_mom_crosstalk_corrected, str)
                else self.flapwise_bend_mom_crosstalk_corrected.external_id
            )

        if self.flapwise_bend_mom_offset is not None or write_none:
            properties["flapwise_bend_mom_offset"] = (
                self.flapwise_bend_mom_offset
                if isinstance(self.flapwise_bend_mom_offset, str)
                else self.flapwise_bend_mom_offset.external_id
            )

        if self.flapwise_bend_mom_offset_crosstalk_corrected is not None or write_none:
            properties["flapwise_bend_mom_offset_crosstalk_corrected"] = (
                self.flapwise_bend_mom_offset_crosstalk_corrected
                if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, str)
                else self.flapwise_bend_mom_offset_crosstalk_corrected.external_id
            )

        if self.position is not None or write_none:
            properties["position"] = self.position

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
