from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

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
    QueryCore,
    NodeQueryCore,
    StringFilter,
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
    "HighSpeedShaft",
    "HighSpeedShaftWrite",
    "HighSpeedShaftApply",
    "HighSpeedShaftList",
    "HighSpeedShaftWriteList",
    "HighSpeedShaftApplyList",
    "HighSpeedShaftGraphQL",
]


HighSpeedShaftTextFields = Literal["external_id",]
HighSpeedShaftFields = Literal["external_id",]

_HIGHSPEEDSHAFT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class HighSpeedShaftGraphQL(GraphQLCore):
    """This represents the reading version of high speed shaft, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the high speed shaft.
        data_record: The data record of the high speed shaft node.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        nacelle: The nacelle field.
        torque: The torque field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "HighSpeedShaft", "1")
    bending_moment_y: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    bending_monent_x: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
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

    @field_validator("bending_moment_y", "bending_monent_x", "nacelle", "torque", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> HighSpeedShaft:
        """Convert this GraphQL format of high speed shaft to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return HighSpeedShaft(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            bending_moment_y=(
                self.bending_moment_y.as_read()
                if isinstance(self.bending_moment_y, GraphQLCore)
                else self.bending_moment_y
            ),
            bending_monent_x=(
                self.bending_monent_x.as_read()
                if isinstance(self.bending_monent_x, GraphQLCore)
                else self.bending_monent_x
            ),
            nacelle=self.nacelle.as_read() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
            torque=self.torque.as_read() if isinstance(self.torque, GraphQLCore) else self.torque,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> HighSpeedShaftWrite:
        """Convert this GraphQL format of high speed shaft to the writing format."""
        return HighSpeedShaftWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            bending_moment_y=(
                self.bending_moment_y.as_write()
                if isinstance(self.bending_moment_y, GraphQLCore)
                else self.bending_moment_y
            ),
            bending_monent_x=(
                self.bending_monent_x.as_write()
                if isinstance(self.bending_monent_x, GraphQLCore)
                else self.bending_monent_x
            ),
            torque=self.torque.as_write() if isinstance(self.torque, GraphQLCore) else self.torque,
        )


class HighSpeedShaft(DomainModel):
    """This represents the reading version of high speed shaft.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the high speed shaft.
        data_record: The data record of the high speed shaft node.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        nacelle: The nacelle field.
        torque: The torque field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "HighSpeedShaft", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    bending_moment_y: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    bending_monent_x: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    nacelle: Optional[Nacelle] = Field(default=None, repr=False)
    torque: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> HighSpeedShaftWrite:
        """Convert this read version of high speed shaft to the writing version."""
        return HighSpeedShaftWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            bending_moment_y=(
                self.bending_moment_y.as_write()
                if isinstance(self.bending_moment_y, DomainModel)
                else self.bending_moment_y
            ),
            bending_monent_x=(
                self.bending_monent_x.as_write()
                if isinstance(self.bending_monent_x, DomainModel)
                else self.bending_monent_x
            ),
            torque=self.torque.as_write() if isinstance(self.torque, DomainModel) else self.torque,
        )

    def as_apply(self) -> HighSpeedShaftWrite:
        """Convert this read version of high speed shaft to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, HighSpeedShaft],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._nacelle import Nacelle
        from ._sensor_time_series import SensorTimeSeries

        for instance in instances.values():
            if (
                isinstance(instance.bending_moment_y, dm.NodeId | str)
                and (bending_moment_y := nodes_by_id.get(instance.bending_moment_y))
                and isinstance(bending_moment_y, SensorTimeSeries)
            ):
                instance.bending_moment_y = bending_moment_y
            if (
                isinstance(instance.bending_monent_x, dm.NodeId | str)
                and (bending_monent_x := nodes_by_id.get(instance.bending_monent_x))
                and isinstance(bending_monent_x, SensorTimeSeries)
            ):
                instance.bending_monent_x = bending_monent_x
            if (
                isinstance(instance.torque, dm.NodeId | str)
                and (torque := nodes_by_id.get(instance.torque))
                and isinstance(torque, SensorTimeSeries)
            ):
                instance.torque = torque
        for node in nodes_by_id.values():
            if (
                isinstance(node, Nacelle)
                and node.high_speed_shaft is not None
                and (high_speed_shaft := instances.get(as_pygen_node_id(node.high_speed_shaft)))
            ):
                if high_speed_shaft.nacelle is None:
                    high_speed_shaft.nacelle = node
                elif are_nodes_equal(node, high_speed_shaft.nacelle):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'nacelle' in {high_speed_shaft.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {high_speed_shaft.nacelle!s}.",
                        stacklevel=2,
                    )


class HighSpeedShaftWrite(DomainModelWrite):
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "HighSpeedShaft", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    bending_moment_y: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    bending_monent_x: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    torque: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("bending_moment_y", "bending_monent_x", "torque", mode="before")
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

        if self.bending_moment_y is not None:
            properties["bending_moment_y"] = {
                "space": self.space if isinstance(self.bending_moment_y, str) else self.bending_moment_y.space,
                "externalId": (
                    self.bending_moment_y
                    if isinstance(self.bending_moment_y, str)
                    else self.bending_moment_y.external_id
                ),
            }

        if self.bending_monent_x is not None:
            properties["bending_monent_x"] = {
                "space": self.space if isinstance(self.bending_monent_x, str) else self.bending_monent_x.space,
                "externalId": (
                    self.bending_monent_x
                    if isinstance(self.bending_monent_x, str)
                    else self.bending_monent_x.external_id
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

        if isinstance(self.bending_moment_y, DomainModelWrite):
            other_resources = self.bending_moment_y._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.bending_monent_x, DomainModelWrite):
            other_resources = self.bending_monent_x._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.torque, DomainModelWrite):
            other_resources = self.torque._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class HighSpeedShaftApply(HighSpeedShaftWrite):
    def __new__(cls, *args, **kwargs) -> HighSpeedShaftApply:
        warnings.warn(
            "HighSpeedShaftApply is deprecated and will be removed in v1.0. "
            "Use HighSpeedShaftWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "HighSpeedShaft.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class HighSpeedShaftList(DomainModelList[HighSpeedShaft]):
    """List of high speed shafts in the read version."""

    _INSTANCE = HighSpeedShaft

    def as_write(self) -> HighSpeedShaftWriteList:
        """Convert these read versions of high speed shaft to the writing versions."""
        return HighSpeedShaftWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> HighSpeedShaftWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def bending_moment_y(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.bending_moment_y for item in self.data if isinstance(item.bending_moment_y, SensorTimeSeries)]
        )

    @property
    def bending_monent_x(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.bending_monent_x for item in self.data if isinstance(item.bending_monent_x, SensorTimeSeries)]
        )

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])

    @property
    def torque(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList([item.torque for item in self.data if isinstance(item.torque, SensorTimeSeries)])


class HighSpeedShaftWriteList(DomainModelWriteList[HighSpeedShaftWrite]):
    """List of high speed shafts in the writing version."""

    _INSTANCE = HighSpeedShaftWrite

    @property
    def bending_moment_y(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.bending_moment_y for item in self.data if isinstance(item.bending_moment_y, SensorTimeSeriesWrite)]
        )

    @property
    def bending_monent_x(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.bending_monent_x for item in self.data if isinstance(item.bending_monent_x, SensorTimeSeriesWrite)]
        )

    @property
    def torque(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.torque for item in self.data if isinstance(item.torque, SensorTimeSeriesWrite)]
        )


class HighSpeedShaftApplyList(HighSpeedShaftWriteList): ...


def _create_high_speed_shaft_filter(
    view_id: dm.ViewId,
    bending_moment_y: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    bending_monent_x: (
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
    if isinstance(bending_moment_y, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bending_moment_y):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("bending_moment_y"), value=as_instance_dict_id(bending_moment_y))
        )
    if (
        bending_moment_y
        and isinstance(bending_moment_y, Sequence)
        and not isinstance(bending_moment_y, str)
        and not is_tuple_id(bending_moment_y)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bending_moment_y"),
                values=[as_instance_dict_id(item) for item in bending_moment_y],
            )
        )
    if isinstance(bending_monent_x, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bending_monent_x):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("bending_monent_x"), value=as_instance_dict_id(bending_monent_x))
        )
    if (
        bending_monent_x
        and isinstance(bending_monent_x, Sequence)
        and not isinstance(bending_monent_x, str)
        and not is_tuple_id(bending_monent_x)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bending_monent_x"),
                values=[as_instance_dict_id(item) for item in bending_monent_x],
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


class _HighSpeedShaftQuery(NodeQueryCore[T_DomainModelList, HighSpeedShaftList]):
    _view_id = HighSpeedShaft._view_id
    _result_cls = HighSpeedShaft
    _result_list_cls_end = HighSpeedShaftList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _SensorTimeSeriesQuery not in created_types:
            self.bending_moment_y = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bending_moment_y"),
                    direction="outwards",
                ),
                connection_name="bending_moment_y",
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.bending_monent_x = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bending_monent_x"),
                    direction="outwards",
                ),
                connection_name="bending_monent_x",
            )

        if _NacelleQuery not in created_types:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "Nacelle", "1").as_property_ref("high_speed_shaft"),
                    direction="inwards",
                ),
                connection_name="nacelle",
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_high_speed_shaft(self, limit: int = DEFAULT_QUERY_LIMIT) -> HighSpeedShaftList:
        return self._list(limit=limit)


class HighSpeedShaftQuery(_HighSpeedShaftQuery[HighSpeedShaftList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, HighSpeedShaftList)
