from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, TYPE_CHECKING, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field

from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainRelation,
    DomainRelationWrite,
    DomainRelationList,
    DomainRelationWriteList,
    GraphQLCore,
    ResourcesWrite,
    DomainModelList,
    T_DomainList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_read_args,
    as_write_args,
    as_pygen_node_id,
    is_tuple_id,
    EdgeQueryCore,
    NodeQueryCore,
    QueryCore,
    StringFilter,
    ViewPropertyId,
    FloatFilter,
)
from wind_turbine.data_classes._wind_turbine import WindTurbineWrite
from wind_turbine.data_classes._metmast import Metmast, MetmastGraphQL, MetmastWrite

if TYPE_CHECKING:
    from wind_turbine.data_classes._metmast import Metmast, MetmastGraphQL, MetmastWrite
    from wind_turbine.data_classes._wind_turbine import WindTurbine, WindTurbineGraphQL, WindTurbineWrite


__all__ = [
    "Distance",
    "DistanceWrite",
    "DistanceList",
    "DistanceWriteList",
    "DistanceFields",
]


DistanceTextFields = Literal["external_id",]
DistanceFields = Literal["external_id", "distance"]
_DISTANCE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "distance": "distance",
}


class DistanceGraphQL(GraphQLCore):
    """This represents the reading version of distance, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the distance.
        data_record: The data record of the distance node.
        end_node: The end node of this edge.
        distance: The distance field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Distance", "1")
    end_node: Union[MetmastGraphQL, WindTurbineGraphQL, None] = Field(None, alias="endNode")
    distance: Optional[float] = None

    def as_read(self) -> Distance:
        """Convert this GraphQL format of distance to the reading format."""
        return Distance.model_validate(as_read_args(self))

    def as_write(self) -> DistanceWrite:
        """Convert this GraphQL format of distance to the writing format."""
        return DistanceWrite.model_validate(as_write_args(self))


class Distance(DomainRelation):
    """This represents the reading version of distance.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the distance.
        data_record: The data record of the distance edge.
        end_node: The end node of this edge.
        distance: The distance field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Distance", "1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[Metmast, WindTurbine, str, dm.NodeId] = Field(alias="endNode")
    distance: Optional[float] = None

    def as_write(self) -> DistanceWrite:
        """Convert this read version of distance to the writing version."""
        return DistanceWrite.model_validate(as_write_args(self))


_EXPECTED_START_NODES_BY_END_NODE: dict[type[DomainModelWrite], set[type[DomainModelWrite]]] = {
    MetmastWrite: {WindTurbineWrite},
}


def _validate_end_node(
    start_node: DomainModelWrite, end_node: Union[MetmastWrite, WindTurbineWrite, str, dm.NodeId]
) -> None:
    if isinstance(end_node, str | dm.NodeId):
        # Nothing to validate
        return
    if type(end_node) not in _EXPECTED_START_NODES_BY_END_NODE:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Should be one of {[t.__name__ for t in _EXPECTED_START_NODES_BY_END_NODE.keys()]}"
        )
    if type(start_node) not in _EXPECTED_START_NODES_BY_END_NODE[type(end_node)]:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Expected one of: {_EXPECTED_START_NODES_BY_END_NODE[type(end_node)]}"
        )


class DistanceWrite(DomainRelationWrite):
    """This represents the writing version of distance.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the distance.
        data_record: The data record of the distance edge.
        end_node: The end node of this edge.
        distance: The distance field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("distance",)
    _validate_end_node = _validate_end_node

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Distance", "1")
    end_node: Union[MetmastWrite, WindTurbineWrite, str, dm.NodeId] = Field(alias="endNode")
    distance: Optional[float] = None


class DistanceList(DomainRelationList[Distance]):
    """List of distances in the reading version."""

    _INSTANCE = Distance

    def as_write(self) -> DistanceWriteList:
        """Convert this read version of distance list to the writing version."""
        return DistanceWriteList([edge.as_write() for edge in self])


class DistanceWriteList(DomainRelationWriteList[DistanceWrite]):
    """List of distances in the writing version."""

    _INSTANCE = DistanceWrite


def _create_distance_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    min_distance: float | None = None,
    max_distance: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, str):
        filters.append(
            dm.filters.Equals(["edge", "startNode"], value={"space": start_node_space, "externalId": start_node})
        )
    if start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    (
                        {"space": start_node_space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    if end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    (
                        {"space": space_end_node, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in end_node
                ],
            )
        )
    if min_distance is not None or max_distance is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("distance"), gte=min_distance, lte=max_distance))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class _DistanceQuery(EdgeQueryCore[T_DomainList, DistanceList]):
    _view_id = Distance._view_id
    _result_cls = Distance
    _result_list_cls_end = DistanceList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        end_node_cls: type[NodeQueryCore],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
    ):
        from ._metmast import _MetmastQuery
        from ._wind_turbine import _WindTurbineQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            None,
            connection_name,
            connection_property,
        )
        if end_node_cls not in created_types:
            self.end_node = end_node_cls(
                created_types=created_types.copy(),
                creation_path=self._creation_path,
                client=client,
                result_list_cls=result_list_cls,  # type: ignore[type-var]
                expression=dm.query.NodeResultSetExpression(),
                connection_property=ViewPropertyId(self._view_id, "end_node"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.distance = FloatFilter(self, self._view_id.as_property_ref("distance"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.distance,
            ]
        )
