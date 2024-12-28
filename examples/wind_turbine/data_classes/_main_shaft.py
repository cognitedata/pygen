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
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._nacelle import Nacelle, NacelleList, NacelleGraphQL, NacelleWrite, NacelleWriteList
    from wind_turbine.data_classes._sensor_time_series import (
        SensorTimeSeries,
        SensorTimeSeriesList,
        SensorTimeSeriesGraphQL,
        SensorTimeSeriesWrite,
        SensorTimeSeriesWriteList,
    )


__all__ = [
    "MainShaft",
    "MainShaftWrite",
    "MainShaftApply",
    "MainShaftList",
    "MainShaftWriteList",
    "MainShaftApplyList",
    "MainShaftGraphQL",
]


MainShaftTextFields = Literal["external_id",]
MainShaftFields = Literal["external_id",]

_MAINSHAFT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class MainShaftGraphQL(GraphQLCore):
    """This represents the reading version of main shaft, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main shaft.
        data_record: The data record of the main shaft node.
        bending_x: The bending x field.
        bending_y: The bending y field.
        calculated_tilt_moment: The calculated tilt moment field.
        calculated_yaw_moment: The calculated yaw moment field.
        nacelle: The nacelle field.
        torque: The torque field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "MainShaft", "1")
    bending_x: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    bending_y: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    calculated_tilt_moment: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    calculated_yaw_moment: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)
    torque: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)

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
        "bending_x", "bending_y", "calculated_tilt_moment", "calculated_yaw_moment", "nacelle", "torque", mode="before"
    )
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> MainShaft:
        """Convert this GraphQL format of main shaft to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return MainShaft(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            bending_x=self.bending_x.as_read() if isinstance(self.bending_x, GraphQLCore) else self.bending_x,
            bending_y=self.bending_y.as_read() if isinstance(self.bending_y, GraphQLCore) else self.bending_y,
            calculated_tilt_moment=(
                self.calculated_tilt_moment.as_read()
                if isinstance(self.calculated_tilt_moment, GraphQLCore)
                else self.calculated_tilt_moment
            ),
            calculated_yaw_moment=(
                self.calculated_yaw_moment.as_read()
                if isinstance(self.calculated_yaw_moment, GraphQLCore)
                else self.calculated_yaw_moment
            ),
            nacelle=self.nacelle.as_read() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
            torque=self.torque.as_read() if isinstance(self.torque, GraphQLCore) else self.torque,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> MainShaftWrite:
        """Convert this GraphQL format of main shaft to the writing format."""
        return MainShaftWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            bending_x=self.bending_x.as_write() if isinstance(self.bending_x, GraphQLCore) else self.bending_x,
            bending_y=self.bending_y.as_write() if isinstance(self.bending_y, GraphQLCore) else self.bending_y,
            calculated_tilt_moment=(
                self.calculated_tilt_moment.as_write()
                if isinstance(self.calculated_tilt_moment, GraphQLCore)
                else self.calculated_tilt_moment
            ),
            calculated_yaw_moment=(
                self.calculated_yaw_moment.as_write()
                if isinstance(self.calculated_yaw_moment, GraphQLCore)
                else self.calculated_yaw_moment
            ),
            torque=self.torque.as_write() if isinstance(self.torque, GraphQLCore) else self.torque,
        )


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
        nacelle: The nacelle field.
        torque: The torque field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "MainShaft", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    bending_x: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    bending_y: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    calculated_tilt_moment: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    calculated_yaw_moment: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    nacelle: Optional[Nacelle] = Field(default=None, repr=False)
    torque: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator(
        "bending_x", "bending_y", "calculated_tilt_moment", "calculated_yaw_moment", "nacelle", "torque", mode="before"
    )
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> MainShaftWrite:
        """Convert this read version of main shaft to the writing version."""
        return MainShaftWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            bending_x=self.bending_x.as_write() if isinstance(self.bending_x, DomainModel) else self.bending_x,
            bending_y=self.bending_y.as_write() if isinstance(self.bending_y, DomainModel) else self.bending_y,
            calculated_tilt_moment=(
                self.calculated_tilt_moment.as_write()
                if isinstance(self.calculated_tilt_moment, DomainModel)
                else self.calculated_tilt_moment
            ),
            calculated_yaw_moment=(
                self.calculated_yaw_moment.as_write()
                if isinstance(self.calculated_yaw_moment, DomainModel)
                else self.calculated_yaw_moment
            ),
            torque=self.torque.as_write() if isinstance(self.torque, DomainModel) else self.torque,
        )

    def as_apply(self) -> MainShaftWrite:
        """Convert this read version of main shaft to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, MainShaft],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._nacelle import Nacelle
        from ._sensor_time_series import SensorTimeSeries

        for instance in instances.values():
            if (
                isinstance(instance.bending_x, dm.NodeId | str)
                and (bending_x := nodes_by_id.get(instance.bending_x))
                and isinstance(bending_x, SensorTimeSeries)
            ):
                instance.bending_x = bending_x
            if (
                isinstance(instance.bending_y, dm.NodeId | str)
                and (bending_y := nodes_by_id.get(instance.bending_y))
                and isinstance(bending_y, SensorTimeSeries)
            ):
                instance.bending_y = bending_y
            if (
                isinstance(instance.calculated_tilt_moment, dm.NodeId | str)
                and (calculated_tilt_moment := nodes_by_id.get(instance.calculated_tilt_moment))
                and isinstance(calculated_tilt_moment, SensorTimeSeries)
            ):
                instance.calculated_tilt_moment = calculated_tilt_moment
            if (
                isinstance(instance.calculated_yaw_moment, dm.NodeId | str)
                and (calculated_yaw_moment := nodes_by_id.get(instance.calculated_yaw_moment))
                and isinstance(calculated_yaw_moment, SensorTimeSeries)
            ):
                instance.calculated_yaw_moment = calculated_yaw_moment
            if (
                isinstance(instance.torque, dm.NodeId | str)
                and (torque := nodes_by_id.get(instance.torque))
                and isinstance(torque, SensorTimeSeries)
            ):
                instance.torque = torque
        for node in nodes_by_id.values():
            if (
                isinstance(node, Nacelle)
                and node.generator is not None
                and (generator := instances.get(as_pygen_node_id(node.generator)))
            ):
                if generator.nacelle is None:
                    generator.nacelle = node
                elif are_nodes_equal(node, generator.nacelle):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'nacelle' in {generator.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {generator.nacelle!s}.",
                        stacklevel=2,
                    )


class MainShaftWrite(DomainModelWrite):
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "MainShaft", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    bending_x: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    bending_y: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    calculated_tilt_moment: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    calculated_yaw_moment: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    torque: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator(
        "bending_x", "bending_y", "calculated_tilt_moment", "calculated_yaw_moment", "torque", mode="before"
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

        if self.bending_x is not None:
            properties["bending_x"] = {
                "space": self.space if isinstance(self.bending_x, str) else self.bending_x.space,
                "externalId": self.bending_x if isinstance(self.bending_x, str) else self.bending_x.external_id,
            }

        if self.bending_y is not None:
            properties["bending_y"] = {
                "space": self.space if isinstance(self.bending_y, str) else self.bending_y.space,
                "externalId": self.bending_y if isinstance(self.bending_y, str) else self.bending_y.external_id,
            }

        if self.calculated_tilt_moment is not None:
            properties["calculated_tilt_moment"] = {
                "space": (
                    self.space if isinstance(self.calculated_tilt_moment, str) else self.calculated_tilt_moment.space
                ),
                "externalId": (
                    self.calculated_tilt_moment
                    if isinstance(self.calculated_tilt_moment, str)
                    else self.calculated_tilt_moment.external_id
                ),
            }

        if self.calculated_yaw_moment is not None:
            properties["calculated_yaw_moment"] = {
                "space": (
                    self.space if isinstance(self.calculated_yaw_moment, str) else self.calculated_yaw_moment.space
                ),
                "externalId": (
                    self.calculated_yaw_moment
                    if isinstance(self.calculated_yaw_moment, str)
                    else self.calculated_yaw_moment.external_id
                ),
            }

        if self.torque is not None:
            properties["torque"] = {
                "space": self.space if isinstance(self.torque, str) else self.torque.space,
                "externalId": self.torque if isinstance(self.torque, str) else self.torque.external_id,
            }

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

        if isinstance(self.bending_x, DomainModelWrite):
            other_resources = self.bending_x._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.bending_y, DomainModelWrite):
            other_resources = self.bending_y._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.calculated_tilt_moment, DomainModelWrite):
            other_resources = self.calculated_tilt_moment._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.calculated_yaw_moment, DomainModelWrite):
            other_resources = self.calculated_yaw_moment._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.torque, DomainModelWrite):
            other_resources = self.torque._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class MainShaftApply(MainShaftWrite):
    def __new__(cls, *args, **kwargs) -> MainShaftApply:
        warnings.warn(
            "MainShaftApply is deprecated and will be removed in v1.0. "
            "Use MainShaftWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "MainShaft.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class MainShaftList(DomainModelList[MainShaft]):
    """List of main shafts in the read version."""

    _INSTANCE = MainShaft

    def as_write(self) -> MainShaftWriteList:
        """Convert these read versions of main shaft to the writing versions."""
        return MainShaftWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> MainShaftWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def bending_x(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.bending_x for item in self.data if isinstance(item.bending_x, SensorTimeSeries)]
        )

    @property
    def bending_y(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.bending_y for item in self.data if isinstance(item.bending_y, SensorTimeSeries)]
        )

    @property
    def calculated_tilt_moment(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.calculated_tilt_moment
                for item in self.data
                if isinstance(item.calculated_tilt_moment, SensorTimeSeries)
            ]
        )

    @property
    def calculated_yaw_moment(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.calculated_yaw_moment
                for item in self.data
                if isinstance(item.calculated_yaw_moment, SensorTimeSeries)
            ]
        )

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])

    @property
    def torque(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList([item.torque for item in self.data if isinstance(item.torque, SensorTimeSeries)])


class MainShaftWriteList(DomainModelWriteList[MainShaftWrite]):
    """List of main shafts in the writing version."""

    _INSTANCE = MainShaftWrite

    @property
    def bending_x(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.bending_x for item in self.data if isinstance(item.bending_x, SensorTimeSeriesWrite)]
        )

    @property
    def bending_y(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.bending_y for item in self.data if isinstance(item.bending_y, SensorTimeSeriesWrite)]
        )

    @property
    def calculated_tilt_moment(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.calculated_tilt_moment
                for item in self.data
                if isinstance(item.calculated_tilt_moment, SensorTimeSeriesWrite)
            ]
        )

    @property
    def calculated_yaw_moment(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.calculated_yaw_moment
                for item in self.data
                if isinstance(item.calculated_yaw_moment, SensorTimeSeriesWrite)
            ]
        )

    @property
    def torque(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.torque for item in self.data if isinstance(item.torque, SensorTimeSeriesWrite)]
        )


class MainShaftApplyList(MainShaftWriteList): ...


def _create_main_shaft_filter(
    view_id: dm.ViewId,
    bending_x: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    bending_y: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    calculated_tilt_moment: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    calculated_yaw_moment: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    torque: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(bending_x, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bending_x):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bending_x"), value=as_instance_dict_id(bending_x)))
    if bending_x and isinstance(bending_x, Sequence) and not isinstance(bending_x, str) and not is_tuple_id(bending_x):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bending_x"), values=[as_instance_dict_id(item) for item in bending_x]
            )
        )
    if isinstance(bending_y, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bending_y):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bending_y"), value=as_instance_dict_id(bending_y)))
    if bending_y and isinstance(bending_y, Sequence) and not isinstance(bending_y, str) and not is_tuple_id(bending_y):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bending_y"), values=[as_instance_dict_id(item) for item in bending_y]
            )
        )
    if isinstance(calculated_tilt_moment, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        calculated_tilt_moment
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("calculated_tilt_moment"), value=as_instance_dict_id(calculated_tilt_moment)
            )
        )
    if (
        calculated_tilt_moment
        and isinstance(calculated_tilt_moment, Sequence)
        and not isinstance(calculated_tilt_moment, str)
        and not is_tuple_id(calculated_tilt_moment)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("calculated_tilt_moment"),
                values=[as_instance_dict_id(item) for item in calculated_tilt_moment],
            )
        )
    if isinstance(calculated_yaw_moment, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        calculated_yaw_moment
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("calculated_yaw_moment"), value=as_instance_dict_id(calculated_yaw_moment)
            )
        )
    if (
        calculated_yaw_moment
        and isinstance(calculated_yaw_moment, Sequence)
        and not isinstance(calculated_yaw_moment, str)
        and not is_tuple_id(calculated_yaw_moment)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("calculated_yaw_moment"),
                values=[as_instance_dict_id(item) for item in calculated_yaw_moment],
            )
        )
    if isinstance(torque, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(torque):
        filters.append(dm.filters.Equals(view_id.as_property_ref("torque"), value=as_instance_dict_id(torque)))
    if torque and isinstance(torque, Sequence) and not isinstance(torque, str) and not is_tuple_id(torque):
        filters.append(
            dm.filters.In(view_id.as_property_ref("torque"), values=[as_instance_dict_id(item) for item in torque])
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _MainShaftQuery(NodeQueryCore[T_DomainModelList, MainShaftList]):
    _view_id = MainShaft._view_id
    _result_cls = MainShaft
    _result_list_cls_end = MainShaftList

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
        from ._nacelle import _NacelleQuery
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

        if _SensorTimeSeriesQuery not in created_types:
            self.bending_x = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bending_x"),
                    direction="outwards",
                ),
                connection_name="bending_x",
                connection_property=ViewPropertyId(self._view_id, "bending_x"),
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.bending_y = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bending_y"),
                    direction="outwards",
                ),
                connection_name="bending_y",
                connection_property=ViewPropertyId(self._view_id, "bending_y"),
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.calculated_tilt_moment = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("calculated_tilt_moment"),
                    direction="outwards",
                ),
                connection_name="calculated_tilt_moment",
                connection_property=ViewPropertyId(self._view_id, "calculated_tilt_moment"),
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.calculated_yaw_moment = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("calculated_yaw_moment"),
                    direction="outwards",
                ),
                connection_name="calculated_yaw_moment",
                connection_property=ViewPropertyId(self._view_id, "calculated_yaw_moment"),
            )

        if _NacelleQuery not in created_types:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "Nacelle", "1").as_property_ref("generator"),
                    direction="inwards",
                ),
                connection_name="nacelle",
                connection_property=ViewPropertyId(self._view_id, "nacelle"),
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.torque = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("torque"),
                    direction="outwards",
                ),
                connection_name="torque",
                connection_property=ViewPropertyId(self._view_id, "torque"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_main_shaft(self, limit: int = DEFAULT_QUERY_LIMIT) -> MainShaftList:
        return self._list(limit=limit)


class MainShaftQuery(_MainShaftQuery[MainShaftList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, MainShaftList)
