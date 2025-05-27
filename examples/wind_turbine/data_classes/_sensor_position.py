from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from wind_turbine.config import global_config
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    DirectRelationFilter,
    FloatFilter,
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._blade import Blade, BladeList, BladeGraphQL, BladeWrite, BladeWriteList
    from wind_turbine.data_classes._sensor_time_series import (
        SensorTimeSeries,
        SensorTimeSeriesList,
        SensorTimeSeriesGraphQL,
        SensorTimeSeriesWrite,
        SensorTimeSeriesWriteList,
    )


__all__ = [
    "SensorPosition",
    "SensorPositionWrite",
    "SensorPositionList",
    "SensorPositionWriteList",
    "SensorPositionFields",
    "SensorPositionGraphQL",
]


SensorPositionTextFields = Literal["external_id",]
SensorPositionFields = Literal["external_id", "position"]

_SENSORPOSITION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
        blade: The blade field.
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SensorPosition", "1")
    blade: Optional[BladeGraphQL] = Field(default=None, repr=False)
    edgewise_bend_mom_crosstalk_corrected: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    edgewise_bend_mom_offset: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    edgewise_bend_mom_offset_crosstalk_corrected: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    edgewisewise_bend_mom: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    flapwise_bend_mom: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    flapwise_bend_mom_crosstalk_corrected: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    flapwise_bend_mom_offset: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    flapwise_bend_mom_offset_crosstalk_corrected: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    position: Optional[float] = None

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @field_validator(
        "blade",
        "edgewise_bend_mom_crosstalk_corrected",
        "edgewise_bend_mom_offset",
        "edgewise_bend_mom_offset_crosstalk_corrected",
        "edgewisewise_bend_mom",
        "flapwise_bend_mom",
        "flapwise_bend_mom_crosstalk_corrected",
        "flapwise_bend_mom_offset",
        "flapwise_bend_mom_offset_crosstalk_corrected",
        mode="before",
    )
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> SensorPosition:
        """Convert this GraphQL format of sensor position to the reading format."""
        return SensorPosition.model_validate(as_read_args(self))

    def as_write(self) -> SensorPositionWrite:
        """Convert this GraphQL format of sensor position to the writing format."""
        return SensorPositionWrite.model_validate(as_write_args(self))


class SensorPosition(DomainModel):
    """This represents the reading version of sensor position.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sensor position.
        data_record: The data record of the sensor position node.
        blade: The blade field.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SensorPosition", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    blade: Union[Blade, str, dm.NodeId, None] = Field(default=None, repr=False)
    edgewise_bend_mom_crosstalk_corrected: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    edgewise_bend_mom_offset: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    edgewise_bend_mom_offset_crosstalk_corrected: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    edgewisewise_bend_mom: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    flapwise_bend_mom: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    flapwise_bend_mom_crosstalk_corrected: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    flapwise_bend_mom_offset: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    flapwise_bend_mom_offset_crosstalk_corrected: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    position: Optional[float] = None

    @field_validator(
        "blade",
        "edgewise_bend_mom_crosstalk_corrected",
        "edgewise_bend_mom_offset",
        "edgewise_bend_mom_offset_crosstalk_corrected",
        "edgewisewise_bend_mom",
        "flapwise_bend_mom",
        "flapwise_bend_mom_crosstalk_corrected",
        "flapwise_bend_mom_offset",
        "flapwise_bend_mom_offset_crosstalk_corrected",
        mode="before",
    )
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> SensorPositionWrite:
        """Convert this read version of sensor position to the writing version."""
        return SensorPositionWrite.model_validate(as_write_args(self))


class SensorPositionWrite(DomainModelWrite):
    """This represents the writing version of sensor position.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sensor position.
        data_record: The data record of the sensor position node.
        blade: The blade field.
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

    _container_fields: ClassVar[tuple[str, ...]] = (
        "blade",
        "edgewise_bend_mom_crosstalk_corrected",
        "edgewise_bend_mom_offset",
        "edgewise_bend_mom_offset_crosstalk_corrected",
        "edgewisewise_bend_mom",
        "flapwise_bend_mom",
        "flapwise_bend_mom_crosstalk_corrected",
        "flapwise_bend_mom_offset",
        "flapwise_bend_mom_offset_crosstalk_corrected",
        "position",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "blade",
        "edgewise_bend_mom_crosstalk_corrected",
        "edgewise_bend_mom_offset",
        "edgewise_bend_mom_offset_crosstalk_corrected",
        "edgewisewise_bend_mom",
        "flapwise_bend_mom",
        "flapwise_bend_mom_crosstalk_corrected",
        "flapwise_bend_mom_offset",
        "flapwise_bend_mom_offset_crosstalk_corrected",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SensorPosition", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    blade: Union[BladeWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    edgewise_bend_mom_crosstalk_corrected: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    edgewise_bend_mom_offset: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    edgewise_bend_mom_offset_crosstalk_corrected: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    edgewisewise_bend_mom: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    flapwise_bend_mom: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    flapwise_bend_mom_crosstalk_corrected: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    flapwise_bend_mom_offset: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    flapwise_bend_mom_offset_crosstalk_corrected: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False
    )
    position: Optional[float] = None

    @field_validator(
        "blade",
        "edgewise_bend_mom_crosstalk_corrected",
        "edgewise_bend_mom_offset",
        "edgewise_bend_mom_offset_crosstalk_corrected",
        "edgewisewise_bend_mom",
        "flapwise_bend_mom",
        "flapwise_bend_mom_crosstalk_corrected",
        "flapwise_bend_mom_offset",
        "flapwise_bend_mom_offset_crosstalk_corrected",
        mode="before",
    )
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class SensorPositionList(DomainModelList[SensorPosition]):
    """List of sensor positions in the read version."""

    _INSTANCE = SensorPosition

    def as_write(self) -> SensorPositionWriteList:
        """Convert these read versions of sensor position to the writing versions."""
        return SensorPositionWriteList([node.as_write() for node in self.data])

    @property
    def blade(self) -> BladeList:
        from ._blade import Blade, BladeList

        return BladeList([item.blade for item in self.data if isinstance(item.blade, Blade)])

    @property
    def edgewise_bend_mom_crosstalk_corrected(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.edgewise_bend_mom_crosstalk_corrected
                for item in self.data
                if isinstance(item.edgewise_bend_mom_crosstalk_corrected, SensorTimeSeries)
            ]
        )

    @property
    def edgewise_bend_mom_offset(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.edgewise_bend_mom_offset
                for item in self.data
                if isinstance(item.edgewise_bend_mom_offset, SensorTimeSeries)
            ]
        )

    @property
    def edgewise_bend_mom_offset_crosstalk_corrected(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.edgewise_bend_mom_offset_crosstalk_corrected
                for item in self.data
                if isinstance(item.edgewise_bend_mom_offset_crosstalk_corrected, SensorTimeSeries)
            ]
        )

    @property
    def edgewisewise_bend_mom(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.edgewisewise_bend_mom
                for item in self.data
                if isinstance(item.edgewisewise_bend_mom, SensorTimeSeries)
            ]
        )

    @property
    def flapwise_bend_mom(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.flapwise_bend_mom for item in self.data if isinstance(item.flapwise_bend_mom, SensorTimeSeries)]
        )

    @property
    def flapwise_bend_mom_crosstalk_corrected(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.flapwise_bend_mom_crosstalk_corrected
                for item in self.data
                if isinstance(item.flapwise_bend_mom_crosstalk_corrected, SensorTimeSeries)
            ]
        )

    @property
    def flapwise_bend_mom_offset(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.flapwise_bend_mom_offset
                for item in self.data
                if isinstance(item.flapwise_bend_mom_offset, SensorTimeSeries)
            ]
        )

    @property
    def flapwise_bend_mom_offset_crosstalk_corrected(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.flapwise_bend_mom_offset_crosstalk_corrected
                for item in self.data
                if isinstance(item.flapwise_bend_mom_offset_crosstalk_corrected, SensorTimeSeries)
            ]
        )


class SensorPositionWriteList(DomainModelWriteList[SensorPositionWrite]):
    """List of sensor positions in the writing version."""

    _INSTANCE = SensorPositionWrite

    @property
    def blade(self) -> BladeWriteList:
        from ._blade import BladeWrite, BladeWriteList

        return BladeWriteList([item.blade for item in self.data if isinstance(item.blade, BladeWrite)])

    @property
    def edgewise_bend_mom_crosstalk_corrected(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.edgewise_bend_mom_crosstalk_corrected
                for item in self.data
                if isinstance(item.edgewise_bend_mom_crosstalk_corrected, SensorTimeSeriesWrite)
            ]
        )

    @property
    def edgewise_bend_mom_offset(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.edgewise_bend_mom_offset
                for item in self.data
                if isinstance(item.edgewise_bend_mom_offset, SensorTimeSeriesWrite)
            ]
        )

    @property
    def edgewise_bend_mom_offset_crosstalk_corrected(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.edgewise_bend_mom_offset_crosstalk_corrected
                for item in self.data
                if isinstance(item.edgewise_bend_mom_offset_crosstalk_corrected, SensorTimeSeriesWrite)
            ]
        )

    @property
    def edgewisewise_bend_mom(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.edgewisewise_bend_mom
                for item in self.data
                if isinstance(item.edgewisewise_bend_mom, SensorTimeSeriesWrite)
            ]
        )

    @property
    def flapwise_bend_mom(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.flapwise_bend_mom for item in self.data if isinstance(item.flapwise_bend_mom, SensorTimeSeriesWrite)]
        )

    @property
    def flapwise_bend_mom_crosstalk_corrected(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.flapwise_bend_mom_crosstalk_corrected
                for item in self.data
                if isinstance(item.flapwise_bend_mom_crosstalk_corrected, SensorTimeSeriesWrite)
            ]
        )

    @property
    def flapwise_bend_mom_offset(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.flapwise_bend_mom_offset
                for item in self.data
                if isinstance(item.flapwise_bend_mom_offset, SensorTimeSeriesWrite)
            ]
        )

    @property
    def flapwise_bend_mom_offset_crosstalk_corrected(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.flapwise_bend_mom_offset_crosstalk_corrected
                for item in self.data
                if isinstance(item.flapwise_bend_mom_offset_crosstalk_corrected, SensorTimeSeriesWrite)
            ]
        )


def _create_sensor_position_filter(
    view_id: dm.ViewId,
    blade: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    edgewise_bend_mom_crosstalk_corrected: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    edgewise_bend_mom_offset: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    edgewise_bend_mom_offset_crosstalk_corrected: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    edgewisewise_bend_mom: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    flapwise_bend_mom: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    flapwise_bend_mom_crosstalk_corrected: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    flapwise_bend_mom_offset: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    flapwise_bend_mom_offset_crosstalk_corrected: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    min_position: float | None = None,
    max_position: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(blade, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(blade):
        filters.append(dm.filters.Equals(view_id.as_property_ref("blade"), value=as_instance_dict_id(blade)))
    if blade and isinstance(blade, Sequence) and not isinstance(blade, str) and not is_tuple_id(blade):
        filters.append(
            dm.filters.In(view_id.as_property_ref("blade"), values=[as_instance_dict_id(item) for item in blade])
        )
    if isinstance(edgewise_bend_mom_crosstalk_corrected, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        edgewise_bend_mom_crosstalk_corrected
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("edgewise_bend_mom_crosstalk_corrected"),
                value=as_instance_dict_id(edgewise_bend_mom_crosstalk_corrected),
            )
        )
    if (
        edgewise_bend_mom_crosstalk_corrected
        and isinstance(edgewise_bend_mom_crosstalk_corrected, Sequence)
        and not isinstance(edgewise_bend_mom_crosstalk_corrected, str)
        and not is_tuple_id(edgewise_bend_mom_crosstalk_corrected)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("edgewise_bend_mom_crosstalk_corrected"),
                values=[as_instance_dict_id(item) for item in edgewise_bend_mom_crosstalk_corrected],
            )
        )
    if isinstance(edgewise_bend_mom_offset, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        edgewise_bend_mom_offset
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("edgewise_bend_mom_offset"), value=as_instance_dict_id(edgewise_bend_mom_offset)
            )
        )
    if (
        edgewise_bend_mom_offset
        and isinstance(edgewise_bend_mom_offset, Sequence)
        and not isinstance(edgewise_bend_mom_offset, str)
        and not is_tuple_id(edgewise_bend_mom_offset)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("edgewise_bend_mom_offset"),
                values=[as_instance_dict_id(item) for item in edgewise_bend_mom_offset],
            )
        )
    if isinstance(
        edgewise_bend_mom_offset_crosstalk_corrected, str | dm.NodeId | dm.DirectRelationReference
    ) or is_tuple_id(edgewise_bend_mom_offset_crosstalk_corrected):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("edgewise_bend_mom_offset_crosstalk_corrected"),
                value=as_instance_dict_id(edgewise_bend_mom_offset_crosstalk_corrected),
            )
        )
    if (
        edgewise_bend_mom_offset_crosstalk_corrected
        and isinstance(edgewise_bend_mom_offset_crosstalk_corrected, Sequence)
        and not isinstance(edgewise_bend_mom_offset_crosstalk_corrected, str)
        and not is_tuple_id(edgewise_bend_mom_offset_crosstalk_corrected)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("edgewise_bend_mom_offset_crosstalk_corrected"),
                values=[as_instance_dict_id(item) for item in edgewise_bend_mom_offset_crosstalk_corrected],
            )
        )
    if isinstance(edgewisewise_bend_mom, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        edgewisewise_bend_mom
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("edgewisewise_bend_mom"), value=as_instance_dict_id(edgewisewise_bend_mom)
            )
        )
    if (
        edgewisewise_bend_mom
        and isinstance(edgewisewise_bend_mom, Sequence)
        and not isinstance(edgewisewise_bend_mom, str)
        and not is_tuple_id(edgewisewise_bend_mom)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("edgewisewise_bend_mom"),
                values=[as_instance_dict_id(item) for item in edgewisewise_bend_mom],
            )
        )
    if isinstance(flapwise_bend_mom, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(flapwise_bend_mom):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("flapwise_bend_mom"), value=as_instance_dict_id(flapwise_bend_mom)
            )
        )
    if (
        flapwise_bend_mom
        and isinstance(flapwise_bend_mom, Sequence)
        and not isinstance(flapwise_bend_mom, str)
        and not is_tuple_id(flapwise_bend_mom)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("flapwise_bend_mom"),
                values=[as_instance_dict_id(item) for item in flapwise_bend_mom],
            )
        )
    if isinstance(flapwise_bend_mom_crosstalk_corrected, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        flapwise_bend_mom_crosstalk_corrected
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("flapwise_bend_mom_crosstalk_corrected"),
                value=as_instance_dict_id(flapwise_bend_mom_crosstalk_corrected),
            )
        )
    if (
        flapwise_bend_mom_crosstalk_corrected
        and isinstance(flapwise_bend_mom_crosstalk_corrected, Sequence)
        and not isinstance(flapwise_bend_mom_crosstalk_corrected, str)
        and not is_tuple_id(flapwise_bend_mom_crosstalk_corrected)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("flapwise_bend_mom_crosstalk_corrected"),
                values=[as_instance_dict_id(item) for item in flapwise_bend_mom_crosstalk_corrected],
            )
        )
    if isinstance(flapwise_bend_mom_offset, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        flapwise_bend_mom_offset
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("flapwise_bend_mom_offset"), value=as_instance_dict_id(flapwise_bend_mom_offset)
            )
        )
    if (
        flapwise_bend_mom_offset
        and isinstance(flapwise_bend_mom_offset, Sequence)
        and not isinstance(flapwise_bend_mom_offset, str)
        and not is_tuple_id(flapwise_bend_mom_offset)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("flapwise_bend_mom_offset"),
                values=[as_instance_dict_id(item) for item in flapwise_bend_mom_offset],
            )
        )
    if isinstance(
        flapwise_bend_mom_offset_crosstalk_corrected, str | dm.NodeId | dm.DirectRelationReference
    ) or is_tuple_id(flapwise_bend_mom_offset_crosstalk_corrected):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("flapwise_bend_mom_offset_crosstalk_corrected"),
                value=as_instance_dict_id(flapwise_bend_mom_offset_crosstalk_corrected),
            )
        )
    if (
        flapwise_bend_mom_offset_crosstalk_corrected
        and isinstance(flapwise_bend_mom_offset_crosstalk_corrected, Sequence)
        and not isinstance(flapwise_bend_mom_offset_crosstalk_corrected, str)
        and not is_tuple_id(flapwise_bend_mom_offset_crosstalk_corrected)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("flapwise_bend_mom_offset_crosstalk_corrected"),
                values=[as_instance_dict_id(item) for item in flapwise_bend_mom_offset_crosstalk_corrected],
            )
        )
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


class _SensorPositionQuery(NodeQueryCore[T_DomainModelList, SensorPositionList]):
    _view_id = SensorPosition._view_id
    _result_cls = SensorPosition
    _result_list_cls_end = SensorPositionList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
    ):
        from ._blade import _BladeQuery
        from ._sensor_time_series import _SensorTimeSeriesQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _BladeQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.blade = _BladeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("blade"),
                    direction="outwards",
                ),
                connection_name="blade",
                connection_property=ViewPropertyId(self._view_id, "blade"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.edgewise_bend_mom_crosstalk_corrected = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("edgewise_bend_mom_crosstalk_corrected"),
                    direction="outwards",
                ),
                connection_name="edgewise_bend_mom_crosstalk_corrected",
                connection_property=ViewPropertyId(self._view_id, "edgewise_bend_mom_crosstalk_corrected"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.edgewise_bend_mom_offset = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("edgewise_bend_mom_offset"),
                    direction="outwards",
                ),
                connection_name="edgewise_bend_mom_offset",
                connection_property=ViewPropertyId(self._view_id, "edgewise_bend_mom_offset"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.edgewise_bend_mom_offset_crosstalk_corrected = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("edgewise_bend_mom_offset_crosstalk_corrected"),
                    direction="outwards",
                ),
                connection_name="edgewise_bend_mom_offset_crosstalk_corrected",
                connection_property=ViewPropertyId(self._view_id, "edgewise_bend_mom_offset_crosstalk_corrected"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.edgewisewise_bend_mom = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("edgewisewise_bend_mom"),
                    direction="outwards",
                ),
                connection_name="edgewisewise_bend_mom",
                connection_property=ViewPropertyId(self._view_id, "edgewisewise_bend_mom"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.flapwise_bend_mom = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("flapwise_bend_mom"),
                    direction="outwards",
                ),
                connection_name="flapwise_bend_mom",
                connection_property=ViewPropertyId(self._view_id, "flapwise_bend_mom"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.flapwise_bend_mom_crosstalk_corrected = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("flapwise_bend_mom_crosstalk_corrected"),
                    direction="outwards",
                ),
                connection_name="flapwise_bend_mom_crosstalk_corrected",
                connection_property=ViewPropertyId(self._view_id, "flapwise_bend_mom_crosstalk_corrected"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.flapwise_bend_mom_offset = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("flapwise_bend_mom_offset"),
                    direction="outwards",
                ),
                connection_name="flapwise_bend_mom_offset",
                connection_property=ViewPropertyId(self._view_id, "flapwise_bend_mom_offset"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.flapwise_bend_mom_offset_crosstalk_corrected = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("flapwise_bend_mom_offset_crosstalk_corrected"),
                    direction="outwards",
                ),
                connection_name="flapwise_bend_mom_offset_crosstalk_corrected",
                connection_property=ViewPropertyId(self._view_id, "flapwise_bend_mom_offset_crosstalk_corrected"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.blade_filter = DirectRelationFilter(self, self._view_id.as_property_ref("blade"))
        self.edgewise_bend_mom_crosstalk_corrected_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("edgewise_bend_mom_crosstalk_corrected")
        )
        self.edgewise_bend_mom_offset_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("edgewise_bend_mom_offset")
        )
        self.edgewise_bend_mom_offset_crosstalk_corrected_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("edgewise_bend_mom_offset_crosstalk_corrected")
        )
        self.edgewisewise_bend_mom_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("edgewisewise_bend_mom")
        )
        self.flapwise_bend_mom_filter = DirectRelationFilter(self, self._view_id.as_property_ref("flapwise_bend_mom"))
        self.flapwise_bend_mom_crosstalk_corrected_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("flapwise_bend_mom_crosstalk_corrected")
        )
        self.flapwise_bend_mom_offset_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("flapwise_bend_mom_offset")
        )
        self.flapwise_bend_mom_offset_crosstalk_corrected_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("flapwise_bend_mom_offset_crosstalk_corrected")
        )
        self.position = FloatFilter(self, self._view_id.as_property_ref("position"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.blade_filter,
                self.edgewise_bend_mom_crosstalk_corrected_filter,
                self.edgewise_bend_mom_offset_filter,
                self.edgewise_bend_mom_offset_crosstalk_corrected_filter,
                self.edgewisewise_bend_mom_filter,
                self.flapwise_bend_mom_filter,
                self.flapwise_bend_mom_crosstalk_corrected_filter,
                self.flapwise_bend_mom_offset_filter,
                self.flapwise_bend_mom_offset_crosstalk_corrected_filter,
                self.position,
            ]
        )

    def list_sensor_position(self, limit: int = DEFAULT_QUERY_LIMIT) -> SensorPositionList:
        return self._list(limit=limit)


class SensorPositionQuery(_SensorPositionQuery[SensorPositionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, SensorPositionList)
