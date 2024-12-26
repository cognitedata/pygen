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
    "Gearbox",
    "GearboxWrite",
    "GearboxApply",
    "GearboxList",
    "GearboxWriteList",
    "GearboxApplyList",
    "GearboxGraphQL",
]


GearboxTextFields = Literal["external_id",]
GearboxFields = Literal["external_id",]

_GEARBOX_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class GearboxGraphQL(GraphQLCore):
    """This represents the reading version of gearbox, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
        nacelle: The nacelle field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Gearbox", "1")
    displacement_x: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    displacement_y: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    displacement_z: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)

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

    @field_validator("displacement_x", "displacement_y", "displacement_z", "nacelle", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Gearbox:
        """Convert this GraphQL format of gearbox to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Gearbox(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            displacement_x=(
                self.displacement_x.as_read() if isinstance(self.displacement_x, GraphQLCore) else self.displacement_x
            ),
            displacement_y=(
                self.displacement_y.as_read() if isinstance(self.displacement_y, GraphQLCore) else self.displacement_y
            ),
            displacement_z=(
                self.displacement_z.as_read() if isinstance(self.displacement_z, GraphQLCore) else self.displacement_z
            ),
            nacelle=self.nacelle.as_read() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GearboxWrite:
        """Convert this GraphQL format of gearbox to the writing format."""
        return GearboxWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            displacement_x=(
                self.displacement_x.as_write() if isinstance(self.displacement_x, GraphQLCore) else self.displacement_x
            ),
            displacement_y=(
                self.displacement_y.as_write() if isinstance(self.displacement_y, GraphQLCore) else self.displacement_y
            ),
            displacement_z=(
                self.displacement_z.as_write() if isinstance(self.displacement_z, GraphQLCore) else self.displacement_z
            ),
        )


class Gearbox(DomainModel):
    """This represents the reading version of gearbox.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
        nacelle: The nacelle field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Gearbox", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    displacement_x: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_y: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_z: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    nacelle: Optional[Nacelle] = Field(default=None, repr=False)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GearboxWrite:
        """Convert this read version of gearbox to the writing version."""
        return GearboxWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            displacement_x=(
                self.displacement_x.as_write() if isinstance(self.displacement_x, DomainModel) else self.displacement_x
            ),
            displacement_y=(
                self.displacement_y.as_write() if isinstance(self.displacement_y, DomainModel) else self.displacement_y
            ),
            displacement_z=(
                self.displacement_z.as_write() if isinstance(self.displacement_z, DomainModel) else self.displacement_z
            ),
        )

    def as_apply(self) -> GearboxWrite:
        """Convert this read version of gearbox to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Gearbox],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._nacelle import Nacelle
        from ._sensor_time_series import SensorTimeSeries

        for instance in instances.values():
            if (
                isinstance(instance.displacement_x, dm.NodeId | str)
                and (displacement_x := nodes_by_id.get(instance.displacement_x))
                and isinstance(displacement_x, SensorTimeSeries)
            ):
                instance.displacement_x = displacement_x
            if (
                isinstance(instance.displacement_y, dm.NodeId | str)
                and (displacement_y := nodes_by_id.get(instance.displacement_y))
                and isinstance(displacement_y, SensorTimeSeries)
            ):
                instance.displacement_y = displacement_y
            if (
                isinstance(instance.displacement_z, dm.NodeId | str)
                and (displacement_z := nodes_by_id.get(instance.displacement_z))
                and isinstance(displacement_z, SensorTimeSeries)
            ):
                instance.displacement_z = displacement_z
        for node in nodes_by_id.values():
            if (
                isinstance(node, Nacelle)
                and node.gearbox is not None
                and (gearbox := instances.get(as_pygen_node_id(node.gearbox)))
            ):
                if gearbox.nacelle is None:
                    gearbox.nacelle = node
                elif are_nodes_equal(node, gearbox.nacelle):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'nacelle' in {gearbox.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {gearbox.nacelle!s}.",
                        stacklevel=2,
                    )


class GearboxWrite(DomainModelWrite):
    """This represents the writing version of gearbox.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Gearbox", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    displacement_x: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_y: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_z: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("displacement_x", "displacement_y", "displacement_z", mode="before")
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

        if self.displacement_x is not None:
            properties["displacement_x"] = {
                "space": self.space if isinstance(self.displacement_x, str) else self.displacement_x.space,
                "externalId": (
                    self.displacement_x if isinstance(self.displacement_x, str) else self.displacement_x.external_id
                ),
            }

        if self.displacement_y is not None:
            properties["displacement_y"] = {
                "space": self.space if isinstance(self.displacement_y, str) else self.displacement_y.space,
                "externalId": (
                    self.displacement_y if isinstance(self.displacement_y, str) else self.displacement_y.external_id
                ),
            }

        if self.displacement_z is not None:
            properties["displacement_z"] = {
                "space": self.space if isinstance(self.displacement_z, str) else self.displacement_z.space,
                "externalId": (
                    self.displacement_z if isinstance(self.displacement_z, str) else self.displacement_z.external_id
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

        if isinstance(self.displacement_x, DomainModelWrite):
            other_resources = self.displacement_x._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.displacement_y, DomainModelWrite):
            other_resources = self.displacement_y._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.displacement_z, DomainModelWrite):
            other_resources = self.displacement_z._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class GearboxApply(GearboxWrite):
    def __new__(cls, *args, **kwargs) -> GearboxApply:
        warnings.warn(
            "GearboxApply is deprecated and will be removed in v1.0. "
            "Use GearboxWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Gearbox.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class GearboxList(DomainModelList[Gearbox]):
    """List of gearboxes in the read version."""

    _INSTANCE = Gearbox

    def as_write(self) -> GearboxWriteList:
        """Convert these read versions of gearbox to the writing versions."""
        return GearboxWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> GearboxWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def displacement_x(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.displacement_x for item in self.data if isinstance(item.displacement_x, SensorTimeSeries)]
        )

    @property
    def displacement_y(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.displacement_y for item in self.data if isinstance(item.displacement_y, SensorTimeSeries)]
        )

    @property
    def displacement_z(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.displacement_z for item in self.data if isinstance(item.displacement_z, SensorTimeSeries)]
        )

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])


class GearboxWriteList(DomainModelWriteList[GearboxWrite]):
    """List of gearboxes in the writing version."""

    _INSTANCE = GearboxWrite

    @property
    def displacement_x(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.displacement_x for item in self.data if isinstance(item.displacement_x, SensorTimeSeriesWrite)]
        )

    @property
    def displacement_y(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.displacement_y for item in self.data if isinstance(item.displacement_y, SensorTimeSeriesWrite)]
        )

    @property
    def displacement_z(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.displacement_z for item in self.data if isinstance(item.displacement_z, SensorTimeSeriesWrite)]
        )


class GearboxApplyList(GearboxWriteList): ...


def _create_gearbox_filter(
    view_id: dm.ViewId,
    displacement_x: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    displacement_y: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    displacement_z: (
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
    if isinstance(displacement_x, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(displacement_x):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("displacement_x"), value=as_instance_dict_id(displacement_x))
        )
    if (
        displacement_x
        and isinstance(displacement_x, Sequence)
        and not isinstance(displacement_x, str)
        and not is_tuple_id(displacement_x)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("displacement_x"), values=[as_instance_dict_id(item) for item in displacement_x]
            )
        )
    if isinstance(displacement_y, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(displacement_y):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("displacement_y"), value=as_instance_dict_id(displacement_y))
        )
    if (
        displacement_y
        and isinstance(displacement_y, Sequence)
        and not isinstance(displacement_y, str)
        and not is_tuple_id(displacement_y)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("displacement_y"), values=[as_instance_dict_id(item) for item in displacement_y]
            )
        )
    if isinstance(displacement_z, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(displacement_z):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("displacement_z"), value=as_instance_dict_id(displacement_z))
        )
    if (
        displacement_z
        and isinstance(displacement_z, Sequence)
        and not isinstance(displacement_z, str)
        and not is_tuple_id(displacement_z)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("displacement_z"), values=[as_instance_dict_id(item) for item in displacement_z]
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


class _GearboxQuery(NodeQueryCore[T_DomainModelList, GearboxList]):
    _view_id = Gearbox._view_id
    _result_cls = Gearbox
    _result_list_cls_end = GearboxList

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
            self.displacement_x = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("displacement_x"),
                    direction="outwards",
                ),
                connection_name="displacement_x",
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.displacement_y = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("displacement_y"),
                    direction="outwards",
                ),
                connection_name="displacement_y",
            )

        if _SensorTimeSeriesQuery not in created_types:
            self.displacement_z = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("displacement_z"),
                    direction="outwards",
                ),
                connection_name="displacement_z",
            )

        if _NacelleQuery not in created_types:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "Nacelle", "1").as_property_ref("gearbox"),
                    direction="inwards",
                ),
                connection_name="nacelle",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_gearbox(self, limit: int = DEFAULT_QUERY_LIMIT) -> GearboxList:
        return self._list(limit=limit)


class GearboxQuery(_GearboxQuery[GearboxList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, GearboxList)
