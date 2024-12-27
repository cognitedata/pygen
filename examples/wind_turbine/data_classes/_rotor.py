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
)

if TYPE_CHECKING:
    from wind_turbine.data_classes._sensor_time_series import (
        SensorTimeSeries,
        SensorTimeSeriesList,
        SensorTimeSeriesGraphQL,
        SensorTimeSeriesWrite,
        SensorTimeSeriesWriteList,
    )
    from wind_turbine.data_classes._wind_turbine import (
        WindTurbine,
        WindTurbineList,
        WindTurbineGraphQL,
        WindTurbineWrite,
        WindTurbineWriteList,
    )


__all__ = [
    "Rotor",
    "RotorWrite",
    "RotorApply",
    "RotorList",
    "RotorWriteList",
    "RotorApplyList",
    "RotorGraphQL",
]


RotorTextFields = Literal["external_id",]
RotorFields = Literal["external_id",]

_ROTOR_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class RotorGraphQL(GraphQLCore):
    """This represents the reading version of rotor, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
        wind_turbine: The wind turbine field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Rotor", "1")
    rotor_speed_controller: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    rpm_low_speed_shaft: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    wind_turbine: Optional[WindTurbineGraphQL] = Field(default=None, repr=False)

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

    @field_validator("rotor_speed_controller", "rpm_low_speed_shaft", "wind_turbine", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Rotor:
        """Convert this GraphQL format of rotor to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Rotor(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            rotor_speed_controller=(
                self.rotor_speed_controller.as_read()
                if isinstance(self.rotor_speed_controller, GraphQLCore)
                else self.rotor_speed_controller
            ),
            rpm_low_speed_shaft=(
                self.rpm_low_speed_shaft.as_read()
                if isinstance(self.rpm_low_speed_shaft, GraphQLCore)
                else self.rpm_low_speed_shaft
            ),
            wind_turbine=(
                self.wind_turbine.as_read() if isinstance(self.wind_turbine, GraphQLCore) else self.wind_turbine
            ),
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> RotorWrite:
        """Convert this GraphQL format of rotor to the writing format."""
        return RotorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            rotor_speed_controller=(
                self.rotor_speed_controller.as_write()
                if isinstance(self.rotor_speed_controller, GraphQLCore)
                else self.rotor_speed_controller
            ),
            rpm_low_speed_shaft=(
                self.rpm_low_speed_shaft.as_write()
                if isinstance(self.rpm_low_speed_shaft, GraphQLCore)
                else self.rpm_low_speed_shaft
            ),
        )


class Rotor(DomainModel):
    """This represents the reading version of rotor.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
        wind_turbine: The wind turbine field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Rotor", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    rotor_speed_controller: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    rpm_low_speed_shaft: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    wind_turbine: Optional[WindTurbine] = Field(default=None, repr=False)

    @field_validator("rotor_speed_controller", "rpm_low_speed_shaft", "wind_turbine", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> RotorWrite:
        """Convert this read version of rotor to the writing version."""
        return RotorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            rotor_speed_controller=(
                self.rotor_speed_controller.as_write()
                if isinstance(self.rotor_speed_controller, DomainModel)
                else self.rotor_speed_controller
            ),
            rpm_low_speed_shaft=(
                self.rpm_low_speed_shaft.as_write()
                if isinstance(self.rpm_low_speed_shaft, DomainModel)
                else self.rpm_low_speed_shaft
            ),
        )

    def as_apply(self) -> RotorWrite:
        """Convert this read version of rotor to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Rotor],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._sensor_time_series import SensorTimeSeries
        from ._wind_turbine import WindTurbine

        for instance in instances.values():
            if (
                isinstance(instance.rotor_speed_controller, dm.NodeId | str)
                and (rotor_speed_controller := nodes_by_id.get(instance.rotor_speed_controller))
                and isinstance(rotor_speed_controller, SensorTimeSeries)
            ):
                instance.rotor_speed_controller = rotor_speed_controller
            if (
                isinstance(instance.rpm_low_speed_shaft, dm.NodeId | str)
                and (rpm_low_speed_shaft := nodes_by_id.get(instance.rpm_low_speed_shaft))
                and isinstance(rpm_low_speed_shaft, SensorTimeSeries)
            ):
                instance.rpm_low_speed_shaft = rpm_low_speed_shaft
        for node in nodes_by_id.values():
            if (
                isinstance(node, WindTurbine)
                and node.rotor is not None
                and (rotor := instances.get(as_pygen_node_id(node.rotor)))
            ):
                if rotor.wind_turbine is None:
                    rotor.wind_turbine = node
                elif are_nodes_equal(node, rotor.wind_turbine):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'wind_turbine' in {rotor.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {rotor.wind_turbine!s}.",
                        stacklevel=2,
                    )


class RotorWrite(DomainModelWrite):
    """This represents the writing version of rotor.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rotor.
        data_record: The data record of the rotor node.
        rotor_speed_controller: The rotor speed controller field.
        rpm_low_speed_shaft: The rpm low speed shaft field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Rotor", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    rotor_speed_controller: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    rpm_low_speed_shaft: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("rotor_speed_controller", "rpm_low_speed_shaft", mode="before")
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

        if self.rotor_speed_controller is not None:
            properties["rotor_speed_controller"] = {
                "space": (
                    self.space if isinstance(self.rotor_speed_controller, str) else self.rotor_speed_controller.space
                ),
                "externalId": (
                    self.rotor_speed_controller
                    if isinstance(self.rotor_speed_controller, str)
                    else self.rotor_speed_controller.external_id
                ),
            }

        if self.rpm_low_speed_shaft is not None:
            properties["rpm_low_speed_shaft"] = {
                "space": self.space if isinstance(self.rpm_low_speed_shaft, str) else self.rpm_low_speed_shaft.space,
                "externalId": (
                    self.rpm_low_speed_shaft
                    if isinstance(self.rpm_low_speed_shaft, str)
                    else self.rpm_low_speed_shaft.external_id
                ),
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

        if isinstance(self.rotor_speed_controller, DomainModelWrite):
            other_resources = self.rotor_speed_controller._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.rpm_low_speed_shaft, DomainModelWrite):
            other_resources = self.rpm_low_speed_shaft._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class RotorApply(RotorWrite):
    def __new__(cls, *args, **kwargs) -> RotorApply:
        warnings.warn(
            "RotorApply is deprecated and will be removed in v1.0. "
            "Use RotorWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Rotor.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class RotorList(DomainModelList[Rotor]):
    """List of rotors in the read version."""

    _INSTANCE = Rotor

    def as_write(self) -> RotorWriteList:
        """Convert these read versions of rotor to the writing versions."""
        return RotorWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> RotorWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def rotor_speed_controller(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [
                item.rotor_speed_controller
                for item in self.data
                if isinstance(item.rotor_speed_controller, SensorTimeSeries)
            ]
        )

    @property
    def rpm_low_speed_shaft(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.rpm_low_speed_shaft for item in self.data if isinstance(item.rpm_low_speed_shaft, SensorTimeSeries)]
        )

    @property
    def wind_turbine(self) -> WindTurbineList:
        from ._wind_turbine import WindTurbine, WindTurbineList

        return WindTurbineList([item.wind_turbine for item in self.data if isinstance(item.wind_turbine, WindTurbine)])


class RotorWriteList(DomainModelWriteList[RotorWrite]):
    """List of rotors in the writing version."""

    _INSTANCE = RotorWrite

    @property
    def rotor_speed_controller(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.rotor_speed_controller
                for item in self.data
                if isinstance(item.rotor_speed_controller, SensorTimeSeriesWrite)
            ]
        )

    @property
    def rpm_low_speed_shaft(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.rpm_low_speed_shaft
                for item in self.data
                if isinstance(item.rpm_low_speed_shaft, SensorTimeSeriesWrite)
            ]
        )


class RotorApplyList(RotorWriteList): ...


def _create_rotor_filter(
    view_id: dm.ViewId,
    rotor_speed_controller: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    rpm_low_speed_shaft: (
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
    if isinstance(rotor_speed_controller, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        rotor_speed_controller
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rotor_speed_controller"), value=as_instance_dict_id(rotor_speed_controller)
            )
        )
    if (
        rotor_speed_controller
        and isinstance(rotor_speed_controller, Sequence)
        and not isinstance(rotor_speed_controller, str)
        and not is_tuple_id(rotor_speed_controller)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rotor_speed_controller"),
                values=[as_instance_dict_id(item) for item in rotor_speed_controller],
            )
        )
    if isinstance(rpm_low_speed_shaft, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        rpm_low_speed_shaft
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rpm_low_speed_shaft"), value=as_instance_dict_id(rpm_low_speed_shaft)
            )
        )
    if (
        rpm_low_speed_shaft
        and isinstance(rpm_low_speed_shaft, Sequence)
        and not isinstance(rpm_low_speed_shaft, str)
        and not is_tuple_id(rpm_low_speed_shaft)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rpm_low_speed_shaft"),
                values=[as_instance_dict_id(item) for item in rpm_low_speed_shaft],
            )
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


class _RotorQuery(NodeQueryCore[T_DomainModelList, RotorList]):
    _view_id = Rotor._view_id
    _result_cls = Rotor
    _result_list_cls_end = RotorList

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
        from ._sensor_time_series import _SensorTimeSeriesQuery
        from ._wind_turbine import _WindTurbineQuery

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
            self.rotor_speed_controller = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("rotor_speed_controller"),
                    direction="outwards",
                ),
                connection_name="rotor_speed_controller",
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.rpm_low_speed_shaft = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("rpm_low_speed_shaft"),
                    direction="outwards",
                ),
                connection_name="rpm_low_speed_shaft",
            )

        if _WindTurbineQuery not in created_types:
            self.wind_turbine = _WindTurbineQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "WindTurbine", "1").as_property_ref("rotor"),
                    direction="inwards",
                ),
                connection_name="wind_turbine",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_rotor(self, limit: int = DEFAULT_QUERY_LIMIT) -> RotorList:
        return self._list(limit=limit)


class RotorQuery(_RotorQuery[RotorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, RotorList)
