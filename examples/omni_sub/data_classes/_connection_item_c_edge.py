from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, TYPE_CHECKING, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field

from ._core import (
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
    EdgeQueryCore,
    NodeQueryCore,
    QueryCore,
)

if TYPE_CHECKING:
    from ._connection_item_a import ConnectionItemA, ConnectionItemAGraphQL, ConnectionItemAWrite
    from ._connection_item_b import ConnectionItemB, ConnectionItemBGraphQL, ConnectionItemBWrite


__all__ = [
    "ConnectionItemCEdge",
    "ConnectionItemCEdgeWrite",
    "ConnectionItemCEdgeApply",
    "ConnectionItemCEdgeList",
    "ConnectionItemCEdgeWriteList",
    "ConnectionItemCEdgeApplyList",
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemC", "1")
    end_node: Union[dm.NodeId, None] = None
    connection_item_a: Optional[list[ConnectionItemAGraphQL]] = Field(default=None, repr=False, alias="connectionItemA")
    connection_item_b: Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False, alias="connectionItemB")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemCEdge:
        """Convert this GraphQL format of connection item c edge to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemCEdge(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            end_node=self.end_node.as_read() if isinstance(self.end_node, GraphQLCore) else self.end_node,
            connection_item_a=[connection_item_a.as_read() for connection_item_a in self.connection_item_a or []],
            connection_item_b=[connection_item_b.as_read() for connection_item_b in self.connection_item_b or []],
        )


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemC", "1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[str, dm.NodeId]
    connection_item_a: Optional[list[Union[ConnectionItemA, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemB, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    def as_write(self) -> ConnectionItemCEdgeWrite:
        """Convert this read version of connection item c edge to the writing version."""
        return ConnectionItemCEdgeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            end_node=self.end_node,
            connection_item_a=[
                connection_item_a.as_write() if isinstance(connection_item_a, DomainModel) else connection_item_a
                for connection_item_a in self.connection_item_a or []
            ],
            connection_item_b=[
                connection_item_b.as_write() if isinstance(connection_item_b, DomainModel) else connection_item_b
                for connection_item_b in self.connection_item_b or []
            ],
        )

    def as_apply(self) -> ConnectionItemCEdgeWrite:
        """Convert this read version of connection item c edge to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ConnectionItemCEdgeWrite(DomainRelationWrite):
    """This represents the writing version of connection item c edge.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c edge.
        data_record: The data record of the connection item c edge edge.
        end_node: The end node of this edge.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemC", "1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[str, dm.NodeId]
    connection_item_a: Optional[list[Union[ConnectionItemAWrite, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemBWrite, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite,
        edge_type: dm.DirectRelationReference,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.external_id and (self.space, self.external_id) in cache:
            return resources

        _validate_end_node(start_node, self.end_node)

        if isinstance(self.end_node, DomainModelWrite):
            end_node = self.end_node.as_direct_reference()
        elif isinstance(self.end_node, str):
            end_node = dm.DirectRelationReference(self.space, self.end_node)
        elif isinstance(self.end_node, dm.NodeId):
            end_node = dm.DirectRelationReference(self.end_node.space, self.end_node.external_id)
        else:
            raise ValueError(f"Invalid type for equipment_module: {type(self.end_node)}")

        external_id = self.external_id or DomainRelationWrite.external_id_factory(start_node, self.end_node, edge_type)

        properties: dict[str, Any] = {}

        if properties:
            this_edge = dm.EdgeApply(
                space=self.space,
                external_id=external_id,
                type=edge_type,
                start_node=start_node.as_direct_reference(),
                end_node=end_node,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.edges.append(this_edge)
            cache.add((self.space, external_id))

        if isinstance(self.end_node, DomainModelWrite):
            other_resources = self.end_node._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ConnectionItemCEdgeApply(ConnectionItemCEdgeWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemCEdgeApply:
        warnings.warn(
            "ConnectionItemCEdgeApply is deprecated and will be removed in v1.0. Use ConnectionItemCEdgeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemCEdge.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemCEdgeList(DomainRelationList[ConnectionItemCEdge]):
    """List of connection item c edges in the reading version."""

    _INSTANCE = ConnectionItemCEdge

    def as_write(self) -> ConnectionItemCEdgeWriteList:
        """Convert this read version of connection item c edge list to the writing version."""
        return ConnectionItemCEdgeWriteList([edge.as_write() for edge in self])

    def as_apply(self) -> ConnectionItemCEdgeWriteList:
        """Convert these read versions of connection item c edge list to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ConnectionItemCEdgeWriteList(DomainRelationWriteList[ConnectionItemCEdgeWrite]):
    """List of connection item c edges in the writing version."""

    _INSTANCE = ConnectionItemCEdgeWrite


class ConnectionItemCEdgeApplyList(ConnectionItemCEdgeWriteList): ...


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
    elif start_node and isinstance(start_node, dm.NodeId):
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
    elif end_node and isinstance(end_node, dm.NodeId):
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


_EXPECTED_START_NODES_BY_END_NODE: dict[type[DomainModelWrite], set[type[DomainModelWrite]]] = {}


def _validate_end_node(start_node: DomainModelWrite, end_node: Union[str, dm.NodeId]) -> None:
    if isinstance(end_node, (str, dm.NodeId)):
        # Nothing to validate
        return
    if type(end_node) not in _EXPECTED_START_NODES_BY_END_NODE:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. Should be one of {[t.__name__ for t in _EXPECTED_START_NODES_BY_END_NODE.keys()]}"
        )
    if type(start_node) not in _EXPECTED_START_NODES_BY_END_NODE[type(end_node)]:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. Expected one of: {_EXPECTED_START_NODES_BY_END_NODE[type(end_node)]}"
        )


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
    ):
        from ._connection_item_a import _ConnectionItemAQuery
        from ._connection_item_b import _ConnectionItemBQuery

        super().__init__(created_types, creation_path, client, result_list_cls, expression, None, connection_name)
        if end_node_cls not in created_types:
            self.end_node = end_node_cls(
                created_types=created_types.copy(),
                creation_path=self._creation_path,
                client=client,
                result_list_cls=result_list_cls,  # type: ignore[type-var]
                expression=dm.query.NodeResultSetExpression(),
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
                "connection_item_a",
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
                "connection_item_b",
            )
