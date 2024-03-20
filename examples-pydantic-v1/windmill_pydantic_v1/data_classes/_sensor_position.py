from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries
from pydantic import validator, root_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)


__all__ = [
    "SensorPosition",
    "SensorPositionWrite",
    "SensorPositionApply",
    "SensorPositionList",
    "SensorPositionWriteList",
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


class SensorPositionGraphQL(GraphQLCore):
    """This represents the reading version of sensor position, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

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

    view_id = dm.ViewId("power-models", "SensorPosition", "1")
    edgewise_bend_mom_crosstalk_corrected: Union[TimeSeries, str, None] = None
    edgewise_bend_mom_offset: Union[TimeSeries, str, None] = None
    edgewise_bend_mom_offset_crosstalk_corrected: Union[TimeSeries, str, None] = None
    edgewisewise_bend_mom: Union[TimeSeries, str, None] = None
    flapwise_bend_mom: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_crosstalk_corrected: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_offset: Union[TimeSeries, str, None] = None
    flapwise_bend_mom_offset_crosstalk_corrected: Union[TimeSeries, str, None] = None
    position: Optional[float] = None

    @root_validator(pre=True)
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    def as_read(self) -> SensorPosition:
        """Convert this GraphQL format of sensor position to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SensorPosition(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
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

    def as_write(self) -> SensorPositionWrite:
        """Convert this GraphQL format of sensor position to the writing format."""
        return SensorPositionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
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

    def as_write(self) -> SensorPositionWrite:
        """Convert this read version of sensor position to the writing version."""
        return SensorPositionWrite(
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

    def as_apply(self) -> SensorPositionWrite:
        """Convert this read version of sensor position to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SensorPositionWrite(DomainModelWrite):
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

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
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
                properties["edgewise_bend_mom_crosstalk_corrected"] = (
                    self.edgewise_bend_mom_crosstalk_corrected.external_id
                )

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
                properties["edgewise_bend_mom_offset_crosstalk_corrected"] = (
                    self.edgewise_bend_mom_offset_crosstalk_corrected
                )
            else:
                properties["edgewise_bend_mom_offset_crosstalk_corrected"] = (
                    self.edgewise_bend_mom_offset_crosstalk_corrected.external_id
                )

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
                properties["flapwise_bend_mom_crosstalk_corrected"] = (
                    self.flapwise_bend_mom_crosstalk_corrected.external_id
                )

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
                properties["flapwise_bend_mom_offset_crosstalk_corrected"] = (
                    self.flapwise_bend_mom_offset_crosstalk_corrected
                )
            else:
                properties["flapwise_bend_mom_offset_crosstalk_corrected"] = (
                    self.flapwise_bend_mom_offset_crosstalk_corrected.external_id
                )

        if self.position is not None or write_none:
            properties["position"] = self.position

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
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

        if isinstance(self.edgewise_bend_mom_crosstalk_corrected, TimeSeries):
            resources.time_series.append(self.edgewise_bend_mom_crosstalk_corrected)

        if isinstance(self.edgewise_bend_mom_offset, TimeSeries):
            resources.time_series.append(self.edgewise_bend_mom_offset)

        if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, TimeSeries):
            resources.time_series.append(self.edgewise_bend_mom_offset_crosstalk_corrected)

        if isinstance(self.edgewisewise_bend_mom, TimeSeries):
            resources.time_series.append(self.edgewisewise_bend_mom)

        if isinstance(self.flapwise_bend_mom, TimeSeries):
            resources.time_series.append(self.flapwise_bend_mom)

        if isinstance(self.flapwise_bend_mom_crosstalk_corrected, TimeSeries):
            resources.time_series.append(self.flapwise_bend_mom_crosstalk_corrected)

        if isinstance(self.flapwise_bend_mom_offset, TimeSeries):
            resources.time_series.append(self.flapwise_bend_mom_offset)

        if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, TimeSeries):
            resources.time_series.append(self.flapwise_bend_mom_offset_crosstalk_corrected)

        return resources


class SensorPositionApply(SensorPositionWrite):
    def __new__(cls, *args, **kwargs) -> SensorPositionApply:
        warnings.warn(
            "SensorPositionApply is deprecated and will be removed in v1.0. Use SensorPositionWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SensorPosition.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SensorPositionList(DomainModelList[SensorPosition]):
    """List of sensor positions in the read version."""

    _INSTANCE = SensorPosition

    def as_write(self) -> SensorPositionWriteList:
        """Convert these read versions of sensor position to the writing versions."""
        return SensorPositionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SensorPositionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SensorPositionWriteList(DomainModelWriteList[SensorPositionWrite]):
    """List of sensor positions in the writing version."""

    _INSTANCE = SensorPositionWrite


class SensorPositionApplyList(SensorPositionWriteList): ...


def _create_sensor_position_filter(
    view_id: dm.ViewId,
    min_position: float | None = None,
    max_position: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_position is not None or max_position is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("position"), gte=min_position, lte=max_position))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
