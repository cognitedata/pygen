from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, TYPE_CHECKING, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field

from omni.data_classes._core import (
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
)

if TYPE_CHECKING:
    from omni.data_classes._connection_item_a import ConnectionItemA, ConnectionItemAGraphQL, ConnectionItemAWrite
    from omni.data_classes._connection_item_b import ConnectionItemB, ConnectionItemBGraphQL, ConnectionItemBWrite


__all__ = [
    "ConnectionItemCEdge",
    "ConnectionItemCEdgeList",
]


ConnectionItemCEdgeTextFields = Literal["external_id",]
ConnectionItemCEdgeFields = Literal["external_id",]
_CONNECTIONITEMCEDGE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class ConnectionItemCEdgeGraphQL(GraphQLCore):
    """This represents the reading version of connection item c edge, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c edge.
        data_record: The data record of the connection item c edge node.
        end_node: The end node of this edge.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")
    end_node: Union[dm.NodeId, None] = Field(None, alias="endNode")
    connection_item_a: Optional[list[ConnectionItemAGraphQL]] = Field(default=None, repr=False, alias="connectionItemA")
    connection_item_b: Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False, alias="connectionItemB")

    def as_read(self) -> ConnectionItemCEdge:
        """Convert this GraphQL format of connection item c edge to the reading format."""
        return ConnectionItemCEdge.model_validate(as_read_args(self))


class ConnectionItemCEdge(DomainRelation):
    """This represents the reading version of connection item c edge.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c edge.
        data_record: The data record of the connection item c edge edge.
        end_node: The end node of this edge.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[str, dm.NodeId] = Field(alias="endNode")
    connection_item_a: Optional[list[Union[ConnectionItemA, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemB, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )


class ConnectionItemCEdgeList(DomainRelationList[ConnectionItemCEdge]):
    """List of connection item c edges in the reading version."""

    _INSTANCE = ConnectionItemCEdge


def _create_connection_item_c_edge_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class _ConnectionItemCEdgeQuery(EdgeQueryCore[T_DomainList, ConnectionItemCEdgeList]):
    _view_id = ConnectionItemCEdge._view_id
    _result_cls = ConnectionItemCEdge
    _result_list_cls_end = ConnectionItemCEdgeList

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
        from ._connection_item_a import _ConnectionItemAQuery
        from ._connection_item_b import _ConnectionItemBQuery

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

        if _ConnectionItemAQuery not in created_types:
            self.connection_item_a = _ConnectionItemAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,  # type: ignore[type-var]
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="connection_item_a",
                connection_property=ViewPropertyId(self._view_id, "connectionItemA"),
            )

        if _ConnectionItemBQuery not in created_types:
            self.connection_item_b = _ConnectionItemBQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,  # type: ignore[type-var]
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="connection_item_b",
                connection_property=ViewPropertyId(self._view_id, "connectionItemB"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
            ]
        )
