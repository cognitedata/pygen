from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
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
    FloatFilter,
)
if TYPE_CHECKING:
    from wind_turbine.data_classes._distance import Distance, DistanceList, DistanceGraphQL, DistanceWrite, DistanceWriteList


__all__ = [
    "Metmast",
    "MetmastWrite",
    "MetmastApply",
    "MetmastList",
    "MetmastWriteList",
    "MetmastApplyList",
    "MetmastFields",
    "MetmastTextFields",
    "MetmastGraphQL",
]


MetmastTextFields = Literal["external_id", "temperature", "tilt_angle", "wind_speed"]
MetmastFields = Literal["external_id", "position", "temperature", "tilt_angle", "wind_speed"]

_METMAST_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "position": "position",
    "temperature": "temperature",
    "tilt_angle": "tilt_angle",
    "wind_speed": "wind_speed",
}


class MetmastGraphQL(GraphQLCore):
    """This represents the reading version of metmast, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        data_record: The data record of the metmast node.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        wind_turbines: The wind turbine field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Metmast", "1")
    position: Optional[float] = None
    temperature: Optional[TimeSeriesGraphQL] = None
    tilt_angle: Optional[TimeSeriesGraphQL] = None
    wind_speed: Optional[TimeSeriesGraphQL] = None
    wind_turbines: Optional[list[DistanceGraphQL]] = Field(default=None, repr=False)

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


    @field_validator("wind_turbines", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Metmast:
        """Convert this GraphQL format of metmast to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Metmast(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            position=self.position,
            temperature=self.temperature.as_read() if self.temperature else None,
            tilt_angle=self.tilt_angle.as_read() if self.tilt_angle else None,
            wind_speed=self.wind_speed.as_read() if self.wind_speed else None,
            wind_turbines=[wind_turbine.as_read() for wind_turbine in self.wind_turbines] if self.wind_turbines is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> MetmastWrite:
        """Convert this GraphQL format of metmast to the writing format."""
        return MetmastWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            position=self.position,
            temperature=self.temperature.as_write() if self.temperature else None,
            tilt_angle=self.tilt_angle.as_write() if self.tilt_angle else None,
            wind_speed=self.wind_speed.as_write() if self.wind_speed else None,
            wind_turbines=[wind_turbine.as_write() for wind_turbine in self.wind_turbines] if self.wind_turbines is not None else None,
        )


class Metmast(DomainModel):
    """This represents the reading version of metmast.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        data_record: The data record of the metmast node.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        wind_turbines: The wind turbine field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Metmast", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    position: Optional[float] = None
    temperature: Union[TimeSeries, str, None] = None
    tilt_angle: Union[TimeSeries, str, None] = None
    wind_speed: Union[TimeSeries, str, None] = None
    wind_turbines: Optional[list[Distance]] = Field(default=None, repr=False)

    def as_write(self) -> MetmastWrite:
        """Convert this read version of metmast to the writing version."""
        return MetmastWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            position=self.position,
            temperature=self.temperature.as_write() if isinstance(self.temperature, CogniteTimeSeries) else self.temperature,
            tilt_angle=self.tilt_angle.as_write() if isinstance(self.tilt_angle, CogniteTimeSeries) else self.tilt_angle,
            wind_speed=self.wind_speed.as_write() if isinstance(self.wind_speed, CogniteTimeSeries) else self.wind_speed,
            wind_turbines=[wind_turbine.as_write() for wind_turbine in self.wind_turbines] if self.wind_turbines is not None else None,
        )

    def as_apply(self) -> MetmastWrite:
        """Convert this read version of metmast to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Metmast],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._distance import Distance
        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                wind_turbines: list[Distance] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance") and isinstance(
                        value, Distance
                    ):
                        wind_turbines.append(value)
                        if end_node := nodes_by_id.get(as_pygen_node_id(value.end_node)):
                            value.end_node = end_node  # type: ignore[assignment]

                instance.wind_turbines = wind_turbines



class MetmastWrite(DomainModelWrite):
    """This represents the writing version of metmast.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the metmast.
        data_record: The data record of the metmast node.
        position: The position field.
        temperature: The temperature field.
        tilt_angle: The tilt angle field.
        wind_speed: The wind speed field.
        wind_turbines: The wind turbine field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Metmast", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    position: Optional[float] = None
    temperature: Union[TimeSeriesWrite, str, None] = None
    tilt_angle: Union[TimeSeriesWrite, str, None] = None
    wind_speed: Union[TimeSeriesWrite, str, None] = None
    wind_turbines: Optional[list[DistanceWrite]] = Field(default=None, repr=False)

    @field_validator("wind_turbines", mode="before")
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

        if self.position is not None or write_none:
            properties["position"] = self.position

        if self.temperature is not None or write_none:
            properties["temperature"] = self.temperature if isinstance(self.temperature, str) or self.temperature is None else self.temperature.external_id

        if self.tilt_angle is not None or write_none:
            properties["tilt_angle"] = self.tilt_angle if isinstance(self.tilt_angle, str) or self.tilt_angle is None else self.tilt_angle.external_id

        if self.wind_speed is not None or write_none:
            properties["wind_speed"] = self.wind_speed if isinstance(self.wind_speed, str) or self.wind_speed is None else self.wind_speed.external_id

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
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        for wind_turbine in self.wind_turbines or []:
            if isinstance(wind_turbine, DomainRelationWrite):
                other_resources = wind_turbine._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance"),
                )
                resources.extend(other_resources)

        if isinstance(self.temperature, CogniteTimeSeriesWrite):
            resources.time_series.append(self.temperature)

        if isinstance(self.tilt_angle, CogniteTimeSeriesWrite):
            resources.time_series.append(self.tilt_angle)

        if isinstance(self.wind_speed, CogniteTimeSeriesWrite):
            resources.time_series.append(self.wind_speed)

        return resources


class MetmastApply(MetmastWrite):
    def __new__(cls, *args, **kwargs) -> MetmastApply:
        warnings.warn(
            "MetmastApply is deprecated and will be removed in v1.0. Use MetmastWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Metmast.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class MetmastList(DomainModelList[Metmast]):
    """List of metmasts in the read version."""

    _INSTANCE = Metmast
    def as_write(self) -> MetmastWriteList:
        """Convert these read versions of metmast to the writing versions."""
        return MetmastWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> MetmastWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def wind_turbines(self) -> DistanceList:
        from ._distance import Distance, DistanceList
        return DistanceList([item for items in self.data for item in items.wind_turbines or [] if isinstance(item, Distance)])


class MetmastWriteList(DomainModelWriteList[MetmastWrite]):
    """List of metmasts in the writing version."""

    _INSTANCE = MetmastWrite
    @property
    def wind_turbines(self) -> DistanceWriteList:
        from ._distance import DistanceWrite, DistanceWriteList
        return DistanceWriteList([item for items in self.data for item in items.wind_turbines or [] if isinstance(item, DistanceWrite)])


class MetmastApplyList(MetmastWriteList): ...


def _create_metmast_filter(
    view_id: dm.ViewId,
    min_position: float | None = None,
    max_position: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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


class _MetmastQuery(NodeQueryCore[T_DomainModelList, MetmastList]):
    _view_id = Metmast._view_id
    _result_cls = Metmast
    _result_list_cls_end = MetmastList

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
        from ._distance import _DistanceQuery
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

        if _DistanceQuery not in created_types:
            self.wind_turbines = _DistanceQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _WindTurbineQuery,
                dm.query.EdgeResultSetExpression(
                    direction="inwards",
                    chain_to="destination",
                ),
                connection_name="wind_turbines",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.position = FloatFilter(self, self._view_id.as_property_ref("position"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.position,
        ])
        self.temperature = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.temperature if isinstance(item.temperature, str) else item.temperature.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.temperature is not None and
               (isinstance(item.temperature, str) or item.temperature.external_id is not None)
        ])        self.tilt_angle = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.tilt_angle if isinstance(item.tilt_angle, str) else item.tilt_angle.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.tilt_angle is not None and
               (isinstance(item.tilt_angle, str) or item.tilt_angle.external_id is not None)
        ])        self.wind_speed = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.wind_speed if isinstance(item.wind_speed, str) else item.wind_speed.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.wind_speed is not None and
               (isinstance(item.wind_speed, str) or item.wind_speed.external_id is not None)
        ])
    def list_metmast(self, limit: int = DEFAULT_QUERY_LIMIT) -> MetmastList:
        return self._list(limit=limit)


class MetmastQuery(_MetmastQuery[MetmastList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, MetmastList)
