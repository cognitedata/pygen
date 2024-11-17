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
    "PowerInverter",
    "PowerInverterWrite",
    "PowerInverterApply",
    "PowerInverterList",
    "PowerInverterWriteList",
    "PowerInverterApplyList",
    "PowerInverterGraphQL",
]


PowerInverterTextFields = Literal["external_id",]
PowerInverterFields = Literal["external_id",]

_POWERINVERTER_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class PowerInverterGraphQL(GraphQLCore):
    """This represents the reading version of power inverter, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        nacelle: The nacelle field.
        reactive_power_total: The reactive power total field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "PowerInverter", "1")
    active_power_total: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    apparent_power_total: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)
    reactive_power_total: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)

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

    @field_validator("active_power_total", "apparent_power_total", "nacelle", "reactive_power_total", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PowerInverter:
        """Convert this GraphQL format of power inverter to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PowerInverter(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            active_power_total=(
                self.active_power_total.as_read()
                if isinstance(self.active_power_total, GraphQLCore)
                else self.active_power_total
            ),
            apparent_power_total=(
                self.apparent_power_total.as_read()
                if isinstance(self.apparent_power_total, GraphQLCore)
                else self.apparent_power_total
            ),
            nacelle=self.nacelle.as_read() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
            reactive_power_total=(
                self.reactive_power_total.as_read()
                if isinstance(self.reactive_power_total, GraphQLCore)
                else self.reactive_power_total
            ),
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PowerInverterWrite:
        """Convert this GraphQL format of power inverter to the writing format."""
        return PowerInverterWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            active_power_total=(
                self.active_power_total.as_write()
                if isinstance(self.active_power_total, GraphQLCore)
                else self.active_power_total
            ),
            apparent_power_total=(
                self.apparent_power_total.as_write()
                if isinstance(self.apparent_power_total, GraphQLCore)
                else self.apparent_power_total
            ),
            reactive_power_total=(
                self.reactive_power_total.as_write()
                if isinstance(self.reactive_power_total, GraphQLCore)
                else self.reactive_power_total
            ),
        )


class PowerInverter(DomainModel):
    """This represents the reading version of power inverter.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        nacelle: The nacelle field.
        reactive_power_total: The reactive power total field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "PowerInverter", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    active_power_total: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    apparent_power_total: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    nacelle: Optional[Nacelle] = Field(default=None, repr=False)
    reactive_power_total: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)

    def as_write(self) -> PowerInverterWrite:
        """Convert this read version of power inverter to the writing version."""
        return PowerInverterWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            active_power_total=(
                self.active_power_total.as_write()
                if isinstance(self.active_power_total, DomainModel)
                else self.active_power_total
            ),
            apparent_power_total=(
                self.apparent_power_total.as_write()
                if isinstance(self.apparent_power_total, DomainModel)
                else self.apparent_power_total
            ),
            reactive_power_total=(
                self.reactive_power_total.as_write()
                if isinstance(self.reactive_power_total, DomainModel)
                else self.reactive_power_total
            ),
        )

    def as_apply(self) -> PowerInverterWrite:
        """Convert this read version of power inverter to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, PowerInverter],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._nacelle import Nacelle
        from ._sensor_time_series import SensorTimeSeries

        for instance in instances.values():
            if (
                isinstance(instance.active_power_total, (dm.NodeId, str))
                and (active_power_total := nodes_by_id.get(instance.active_power_total))
                and isinstance(active_power_total, SensorTimeSeries)
            ):
                instance.active_power_total = active_power_total
            if (
                isinstance(instance.apparent_power_total, (dm.NodeId, str))
                and (apparent_power_total := nodes_by_id.get(instance.apparent_power_total))
                and isinstance(apparent_power_total, SensorTimeSeries)
            ):
                instance.apparent_power_total = apparent_power_total
            if (
                isinstance(instance.reactive_power_total, (dm.NodeId, str))
                and (reactive_power_total := nodes_by_id.get(instance.reactive_power_total))
                and isinstance(reactive_power_total, SensorTimeSeries)
            ):
                instance.reactive_power_total = reactive_power_total
        for node in nodes_by_id.values():
            if (
                isinstance(node, Nacelle)
                and node.power_inverter is not None
                and (power_inverter := instances.get(as_pygen_node_id(node.power_inverter)))
            ):
                if power_inverter.nacelle is None:
                    power_inverter.nacelle = node
                elif are_nodes_equal(node, power_inverter.nacelle):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'nacelle' in {power_inverter.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {power_inverter.nacelle!s}."
                    )


class PowerInverterWrite(DomainModelWrite):
    """This represents the writing version of power inverter.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        reactive_power_total: The reactive power total field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "PowerInverter", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    active_power_total: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    apparent_power_total: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    reactive_power_total: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("active_power_total", "apparent_power_total", "reactive_power_total", mode="before")
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

        if self.active_power_total is not None:
            properties["active_power_total"] = {
                "space": self.space if isinstance(self.active_power_total, str) else self.active_power_total.space,
                "externalId": (
                    self.active_power_total
                    if isinstance(self.active_power_total, str)
                    else self.active_power_total.external_id
                ),
            }

        if self.apparent_power_total is not None:
            properties["apparent_power_total"] = {
                "space": self.space if isinstance(self.apparent_power_total, str) else self.apparent_power_total.space,
                "externalId": (
                    self.apparent_power_total
                    if isinstance(self.apparent_power_total, str)
                    else self.apparent_power_total.external_id
                ),
            }

        if self.reactive_power_total is not None:
            properties["reactive_power_total"] = {
                "space": self.space if isinstance(self.reactive_power_total, str) else self.reactive_power_total.space,
                "externalId": (
                    self.reactive_power_total
                    if isinstance(self.reactive_power_total, str)
                    else self.reactive_power_total.external_id
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

        if isinstance(self.active_power_total, DomainModelWrite):
            other_resources = self.active_power_total._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.apparent_power_total, DomainModelWrite):
            other_resources = self.apparent_power_total._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.reactive_power_total, DomainModelWrite):
            other_resources = self.reactive_power_total._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class PowerInverterApply(PowerInverterWrite):
    def __new__(cls, *args, **kwargs) -> PowerInverterApply:
        warnings.warn(
            "PowerInverterApply is deprecated and will be removed in v1.0. Use PowerInverterWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PowerInverter.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PowerInverterList(DomainModelList[PowerInverter]):
    """List of power inverters in the read version."""

    _INSTANCE = PowerInverter

    def as_write(self) -> PowerInverterWriteList:
        """Convert these read versions of power inverter to the writing versions."""
        return PowerInverterWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PowerInverterWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def active_power_total(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.active_power_total for item in self.data if isinstance(item.active_power_total, SensorTimeSeries)]
        )

    @property
    def apparent_power_total(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.apparent_power_total for item in self.data if isinstance(item.apparent_power_total, SensorTimeSeries)]
        )

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])

    @property
    def reactive_power_total(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.reactive_power_total for item in self.data if isinstance(item.reactive_power_total, SensorTimeSeries)]
        )


class PowerInverterWriteList(DomainModelWriteList[PowerInverterWrite]):
    """List of power inverters in the writing version."""

    _INSTANCE = PowerInverterWrite

    @property
    def active_power_total(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.active_power_total
                for item in self.data
                if isinstance(item.active_power_total, SensorTimeSeriesWrite)
            ]
        )

    @property
    def apparent_power_total(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.apparent_power_total
                for item in self.data
                if isinstance(item.apparent_power_total, SensorTimeSeriesWrite)
            ]
        )

    @property
    def reactive_power_total(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [
                item.reactive_power_total
                for item in self.data
                if isinstance(item.reactive_power_total, SensorTimeSeriesWrite)
            ]
        )


class PowerInverterApplyList(PowerInverterWriteList): ...


def _create_power_inverter_filter(
    view_id: dm.ViewId,
    active_power_total: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    apparent_power_total: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    reactive_power_total: (
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
    if isinstance(active_power_total, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(active_power_total):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("active_power_total"), value=as_instance_dict_id(active_power_total)
            )
        )
    if (
        active_power_total
        and isinstance(active_power_total, Sequence)
        and not isinstance(active_power_total, str)
        and not is_tuple_id(active_power_total)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("active_power_total"),
                values=[as_instance_dict_id(item) for item in active_power_total],
            )
        )
    if isinstance(apparent_power_total, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        apparent_power_total
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("apparent_power_total"), value=as_instance_dict_id(apparent_power_total)
            )
        )
    if (
        apparent_power_total
        and isinstance(apparent_power_total, Sequence)
        and not isinstance(apparent_power_total, str)
        and not is_tuple_id(apparent_power_total)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("apparent_power_total"),
                values=[as_instance_dict_id(item) for item in apparent_power_total],
            )
        )
    if isinstance(reactive_power_total, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        reactive_power_total
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("reactive_power_total"), value=as_instance_dict_id(reactive_power_total)
            )
        )
    if (
        reactive_power_total
        and isinstance(reactive_power_total, Sequence)
        and not isinstance(reactive_power_total, str)
        and not is_tuple_id(reactive_power_total)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("reactive_power_total"),
                values=[as_instance_dict_id(item) for item in reactive_power_total],
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


class _PowerInverterQuery(NodeQueryCore[T_DomainModelList, PowerInverterList]):
    _view_id = PowerInverter._view_id
    _result_cls = PowerInverter
    _result_list_cls_end = PowerInverterList

    def __init__(
        self,
        created_triples: set[type],
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
            created_triples,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _SensorTimeSeriesQuery not in created_triples:
            self.active_power_total = _SensorTimeSeriesQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("active_power_total"),
                    direction="outwards",
                ),
                connection_name="active_power_total",
            )

        if _SensorTimeSeriesQuery not in created_triples:
            self.apparent_power_total = _SensorTimeSeriesQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("apparent_power_total"),
                    direction="outwards",
                ),
                connection_name="apparent_power_total",
            )

        if _NacelleQuery not in created_triples:
            self.nacelle = _NacelleQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "Nacelle", "1").as_property_ref("power_inverter"),
                    direction="inwards",
                ),
                connection_name="nacelle",
            )

        if _SensorTimeSeriesQuery not in created_triples:
            self.reactive_power_total = _SensorTimeSeriesQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("reactive_power_total"),
                    direction="outwards",
                ),
                connection_name="reactive_power_total",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_power_inverter(self, limit: int = DEFAULT_QUERY_LIMIT) -> PowerInverterList:
        return self._list(limit=limit)


class PowerInverterQuery(_PowerInverterQuery[PowerInverterList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PowerInverterList)
