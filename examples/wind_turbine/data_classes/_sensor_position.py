from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
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
    "SensorPositionApply",
    "SensorPositionList",
    "SensorPositionWriteList",
    "SensorPositionApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
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
            blade=self.blade.as_read() if isinstance(self.blade, GraphQLCore) else self.blade,
            edgewise_bend_mom_crosstalk_corrected=(
                self.edgewise_bend_mom_crosstalk_corrected.as_read()
                if isinstance(self.edgewise_bend_mom_crosstalk_corrected, GraphQLCore)
                else self.edgewise_bend_mom_crosstalk_corrected
            ),
            edgewise_bend_mom_offset=(
                self.edgewise_bend_mom_offset.as_read()
                if isinstance(self.edgewise_bend_mom_offset, GraphQLCore)
                else self.edgewise_bend_mom_offset
            ),
            edgewise_bend_mom_offset_crosstalk_corrected=(
                self.edgewise_bend_mom_offset_crosstalk_corrected.as_read()
                if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, GraphQLCore)
                else self.edgewise_bend_mom_offset_crosstalk_corrected
            ),
            edgewisewise_bend_mom=(
                self.edgewisewise_bend_mom.as_read()
                if isinstance(self.edgewisewise_bend_mom, GraphQLCore)
                else self.edgewisewise_bend_mom
            ),
            flapwise_bend_mom=(
                self.flapwise_bend_mom.as_read()
                if isinstance(self.flapwise_bend_mom, GraphQLCore)
                else self.flapwise_bend_mom
            ),
            flapwise_bend_mom_crosstalk_corrected=(
                self.flapwise_bend_mom_crosstalk_corrected.as_read()
                if isinstance(self.flapwise_bend_mom_crosstalk_corrected, GraphQLCore)
                else self.flapwise_bend_mom_crosstalk_corrected
            ),
            flapwise_bend_mom_offset=(
                self.flapwise_bend_mom_offset.as_read()
                if isinstance(self.flapwise_bend_mom_offset, GraphQLCore)
                else self.flapwise_bend_mom_offset
            ),
            flapwise_bend_mom_offset_crosstalk_corrected=(
                self.flapwise_bend_mom_offset_crosstalk_corrected.as_read()
                if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, GraphQLCore)
                else self.flapwise_bend_mom_offset_crosstalk_corrected
            ),
            position=self.position,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> SensorPositionWrite:
        """Convert this GraphQL format of sensor position to the writing format."""
        return SensorPositionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            blade=self.blade.as_write() if isinstance(self.blade, GraphQLCore) else self.blade,
            edgewise_bend_mom_crosstalk_corrected=(
                self.edgewise_bend_mom_crosstalk_corrected.as_write()
                if isinstance(self.edgewise_bend_mom_crosstalk_corrected, GraphQLCore)
                else self.edgewise_bend_mom_crosstalk_corrected
            ),
            edgewise_bend_mom_offset=(
                self.edgewise_bend_mom_offset.as_write()
                if isinstance(self.edgewise_bend_mom_offset, GraphQLCore)
                else self.edgewise_bend_mom_offset
            ),
            edgewise_bend_mom_offset_crosstalk_corrected=(
                self.edgewise_bend_mom_offset_crosstalk_corrected.as_write()
                if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, GraphQLCore)
                else self.edgewise_bend_mom_offset_crosstalk_corrected
            ),
            edgewisewise_bend_mom=(
                self.edgewisewise_bend_mom.as_write()
                if isinstance(self.edgewisewise_bend_mom, GraphQLCore)
                else self.edgewisewise_bend_mom
            ),
            flapwise_bend_mom=(
                self.flapwise_bend_mom.as_write()
                if isinstance(self.flapwise_bend_mom, GraphQLCore)
                else self.flapwise_bend_mom
            ),
            flapwise_bend_mom_crosstalk_corrected=(
                self.flapwise_bend_mom_crosstalk_corrected.as_write()
                if isinstance(self.flapwise_bend_mom_crosstalk_corrected, GraphQLCore)
                else self.flapwise_bend_mom_crosstalk_corrected
            ),
            flapwise_bend_mom_offset=(
                self.flapwise_bend_mom_offset.as_write()
                if isinstance(self.flapwise_bend_mom_offset, GraphQLCore)
                else self.flapwise_bend_mom_offset
            ),
            flapwise_bend_mom_offset_crosstalk_corrected=(
                self.flapwise_bend_mom_offset_crosstalk_corrected.as_write()
                if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, GraphQLCore)
                else self.flapwise_bend_mom_offset_crosstalk_corrected
            ),
            position=self.position,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> SensorPositionWrite:
        """Convert this read version of sensor position to the writing version."""
        return SensorPositionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            blade=self.blade.as_write() if isinstance(self.blade, DomainModel) else self.blade,
            edgewise_bend_mom_crosstalk_corrected=(
                self.edgewise_bend_mom_crosstalk_corrected.as_write()
                if isinstance(self.edgewise_bend_mom_crosstalk_corrected, DomainModel)
                else self.edgewise_bend_mom_crosstalk_corrected
            ),
            edgewise_bend_mom_offset=(
                self.edgewise_bend_mom_offset.as_write()
                if isinstance(self.edgewise_bend_mom_offset, DomainModel)
                else self.edgewise_bend_mom_offset
            ),
            edgewise_bend_mom_offset_crosstalk_corrected=(
                self.edgewise_bend_mom_offset_crosstalk_corrected.as_write()
                if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, DomainModel)
                else self.edgewise_bend_mom_offset_crosstalk_corrected
            ),
            edgewisewise_bend_mom=(
                self.edgewisewise_bend_mom.as_write()
                if isinstance(self.edgewisewise_bend_mom, DomainModel)
                else self.edgewisewise_bend_mom
            ),
            flapwise_bend_mom=(
                self.flapwise_bend_mom.as_write()
                if isinstance(self.flapwise_bend_mom, DomainModel)
                else self.flapwise_bend_mom
            ),
            flapwise_bend_mom_crosstalk_corrected=(
                self.flapwise_bend_mom_crosstalk_corrected.as_write()
                if isinstance(self.flapwise_bend_mom_crosstalk_corrected, DomainModel)
                else self.flapwise_bend_mom_crosstalk_corrected
            ),
            flapwise_bend_mom_offset=(
                self.flapwise_bend_mom_offset.as_write()
                if isinstance(self.flapwise_bend_mom_offset, DomainModel)
                else self.flapwise_bend_mom_offset
            ),
            flapwise_bend_mom_offset_crosstalk_corrected=(
                self.flapwise_bend_mom_offset_crosstalk_corrected.as_write()
                if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, DomainModel)
                else self.flapwise_bend_mom_offset_crosstalk_corrected
            ),
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

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.blade is not None:
            properties["blade"] = {
                "space": self.space if isinstance(self.blade, str) else self.blade.space,
                "externalId": self.blade if isinstance(self.blade, str) else self.blade.external_id,
            }

        if self.edgewise_bend_mom_crosstalk_corrected is not None:
            properties["edgewise_bend_mom_crosstalk_corrected"] = {
                "space": (
                    self.space
                    if isinstance(self.edgewise_bend_mom_crosstalk_corrected, str)
                    else self.edgewise_bend_mom_crosstalk_corrected.space
                ),
                "externalId": (
                    self.edgewise_bend_mom_crosstalk_corrected
                    if isinstance(self.edgewise_bend_mom_crosstalk_corrected, str)
                    else self.edgewise_bend_mom_crosstalk_corrected.external_id
                ),
            }

        if self.edgewise_bend_mom_offset is not None:
            properties["edgewise_bend_mom_offset"] = {
                "space": (
                    self.space
                    if isinstance(self.edgewise_bend_mom_offset, str)
                    else self.edgewise_bend_mom_offset.space
                ),
                "externalId": (
                    self.edgewise_bend_mom_offset
                    if isinstance(self.edgewise_bend_mom_offset, str)
                    else self.edgewise_bend_mom_offset.external_id
                ),
            }

        if self.edgewise_bend_mom_offset_crosstalk_corrected is not None:
            properties["edgewise_bend_mom_offset_crosstalk_corrected"] = {
                "space": (
                    self.space
                    if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, str)
                    else self.edgewise_bend_mom_offset_crosstalk_corrected.space
                ),
                "externalId": (
                    self.edgewise_bend_mom_offset_crosstalk_corrected
                    if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, str)
                    else self.edgewise_bend_mom_offset_crosstalk_corrected.external_id
                ),
            }

        if self.edgewisewise_bend_mom is not None:
            properties["edgewisewise_bend_mom"] = {
                "space": (
                    self.space if isinstance(self.edgewisewise_bend_mom, str) else self.edgewisewise_bend_mom.space
                ),
                "externalId": (
                    self.edgewisewise_bend_mom
                    if isinstance(self.edgewisewise_bend_mom, str)
                    else self.edgewisewise_bend_mom.external_id
                ),
            }

        if self.flapwise_bend_mom is not None:
            properties["flapwise_bend_mom"] = {
                "space": self.space if isinstance(self.flapwise_bend_mom, str) else self.flapwise_bend_mom.space,
                "externalId": (
                    self.flapwise_bend_mom
                    if isinstance(self.flapwise_bend_mom, str)
                    else self.flapwise_bend_mom.external_id
                ),
            }

        if self.flapwise_bend_mom_crosstalk_corrected is not None:
            properties["flapwise_bend_mom_crosstalk_corrected"] = {
                "space": (
                    self.space
                    if isinstance(self.flapwise_bend_mom_crosstalk_corrected, str)
                    else self.flapwise_bend_mom_crosstalk_corrected.space
                ),
                "externalId": (
                    self.flapwise_bend_mom_crosstalk_corrected
                    if isinstance(self.flapwise_bend_mom_crosstalk_corrected, str)
                    else self.flapwise_bend_mom_crosstalk_corrected.external_id
                ),
            }

        if self.flapwise_bend_mom_offset is not None:
            properties["flapwise_bend_mom_offset"] = {
                "space": (
                    self.space
                    if isinstance(self.flapwise_bend_mom_offset, str)
                    else self.flapwise_bend_mom_offset.space
                ),
                "externalId": (
                    self.flapwise_bend_mom_offset
                    if isinstance(self.flapwise_bend_mom_offset, str)
                    else self.flapwise_bend_mom_offset.external_id
                ),
            }

        if self.flapwise_bend_mom_offset_crosstalk_corrected is not None:
            properties["flapwise_bend_mom_offset_crosstalk_corrected"] = {
                "space": (
                    self.space
                    if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, str)
                    else self.flapwise_bend_mom_offset_crosstalk_corrected.space
                ),
                "externalId": (
                    self.flapwise_bend_mom_offset_crosstalk_corrected
                    if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, str)
                    else self.flapwise_bend_mom_offset_crosstalk_corrected.external_id
                ),
            }

        if self.position is not None or write_none:
            properties["position"] = self.position

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.blade, DomainModelWrite):
            other_resources = self.blade._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.edgewise_bend_mom_crosstalk_corrected, DomainModelWrite):
            other_resources = self.edgewise_bend_mom_crosstalk_corrected._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.edgewise_bend_mom_offset, DomainModelWrite):
            other_resources = self.edgewise_bend_mom_offset._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.edgewise_bend_mom_offset_crosstalk_corrected, DomainModelWrite):
            other_resources = self.edgewise_bend_mom_offset_crosstalk_corrected._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.edgewisewise_bend_mom, DomainModelWrite):
            other_resources = self.edgewisewise_bend_mom._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.flapwise_bend_mom, DomainModelWrite):
            other_resources = self.flapwise_bend_mom._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.flapwise_bend_mom_crosstalk_corrected, DomainModelWrite):
            other_resources = self.flapwise_bend_mom_crosstalk_corrected._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.flapwise_bend_mom_offset, DomainModelWrite):
            other_resources = self.flapwise_bend_mom_offset._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.flapwise_bend_mom_offset_crosstalk_corrected, DomainModelWrite):
            other_resources = self.flapwise_bend_mom_offset_crosstalk_corrected._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class SensorPositionApply(SensorPositionWrite):
    def __new__(cls, *args, **kwargs) -> SensorPositionApply:
        warnings.warn(
            "SensorPositionApply is deprecated and will be removed in v1.0. "
            "Use SensorPositionWrite instead. "
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


class SensorPositionApplyList(SensorPositionWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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

        if _BladeQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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

        if _SensorTimeSeriesQuery not in created_types:
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
        self.position = FloatFilter(self, self._view_id.as_property_ref("position"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.position,
            ]
        )

    def list_sensor_position(self, limit: int = DEFAULT_QUERY_LIMIT) -> SensorPositionList:
        return self._list(limit=limit)


class SensorPositionQuery(_SensorPositionQuery[SensorPositionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, SensorPositionList)
