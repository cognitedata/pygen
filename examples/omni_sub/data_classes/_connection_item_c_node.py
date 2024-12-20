from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from omni_sub.data_classes._core import (
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
    from omni_sub.data_classes._connection_item_a import (
        ConnectionItemA,
        ConnectionItemAList,
        ConnectionItemAGraphQL,
        ConnectionItemAWrite,
        ConnectionItemAWriteList,
    )
    from omni_sub.data_classes._connection_item_b import (
        ConnectionItemB,
        ConnectionItemBList,
        ConnectionItemBGraphQL,
        ConnectionItemBWrite,
        ConnectionItemBWriteList,
    )


__all__ = [
    "ConnectionItemCNode",
    "ConnectionItemCNodeWrite",
    "ConnectionItemCNodeApply",
    "ConnectionItemCNodeList",
    "ConnectionItemCNodeWriteList",
    "ConnectionItemCNodeApplyList",
    "ConnectionItemCNodeGraphQL",
]


ConnectionItemCNodeTextFields = Literal["external_id",]
ConnectionItemCNodeFields = Literal["external_id",]

_CONNECTIONITEMCNODE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class ConnectionItemCNodeGraphQL(GraphQLCore):
    """This represents the reading version of connection item c node, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c node.
        data_record: The data record of the connection item c node node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")
    connection_item_a: Optional[list[ConnectionItemAGraphQL]] = Field(default=None, repr=False, alias="connectionItemA")
    connection_item_b: Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False, alias="connectionItemB")

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

    @field_validator("connection_item_a", "connection_item_b", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemCNode:
        """Convert this GraphQL format of connection item c node to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemCNode(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            connection_item_a=(
                [connection_item_a.as_read() for connection_item_a in self.connection_item_a]
                if self.connection_item_a is not None
                else None
            ),
            connection_item_b=(
                [connection_item_b.as_read() for connection_item_b in self.connection_item_b]
                if self.connection_item_b is not None
                else None
            ),
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemCNodeWrite:
        """Convert this GraphQL format of connection item c node to the writing format."""
        return ConnectionItemCNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            connection_item_a=(
                [connection_item_a.as_write() for connection_item_a in self.connection_item_a]
                if self.connection_item_a is not None
                else None
            ),
            connection_item_b=(
                [connection_item_b.as_write() for connection_item_b in self.connection_item_b]
                if self.connection_item_b is not None
                else None
            ),
        )


class ConnectionItemCNode(DomainModel):
    """This represents the reading version of connection item c node.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c node.
        data_record: The data record of the connection item c node node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")

    space: str
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemC"
    )
    connection_item_a: Optional[list[Union[ConnectionItemA, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemB, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemCNodeWrite:
        """Convert this read version of connection item c node to the writing version."""
        return ConnectionItemCNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            connection_item_a=(
                [
                    connection_item_a.as_write() if isinstance(connection_item_a, DomainModel) else connection_item_a
                    for connection_item_a in self.connection_item_a
                ]
                if self.connection_item_a is not None
                else None
            ),
            connection_item_b=(
                [
                    connection_item_b.as_write() if isinstance(connection_item_b, DomainModel) else connection_item_b
                    for connection_item_b in self.connection_item_b
                ]
                if self.connection_item_b is not None
                else None
            ),
        )

    def as_apply(self) -> ConnectionItemCNodeWrite:
        """Convert this read version of connection item c node to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId, ConnectionItemCNode],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._connection_item_a import ConnectionItemA
        from ._connection_item_b import ConnectionItemB

        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                connection_item_a: list[ConnectionItemA | dm.NodeId] = []
                connection_item_b: list[ConnectionItemB | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId = as_node_id(other_end)
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("sp_pygen_models", "unidirectional") and isinstance(
                        value, (ConnectionItemA, dm.NodeId)
                    ):
                        connection_item_a.append(value)
                    if edge_type == dm.DirectRelationReference("sp_pygen_models", "unidirectional") and isinstance(
                        value, (ConnectionItemB, dm.NodeId)
                    ):
                        connection_item_b.append(value)

                instance.connection_item_a = connection_item_a or None
                instance.connection_item_b = connection_item_b or None


class ConnectionItemCNodeWrite(DomainModelWrite):
    """This represents the writing version of connection item c node.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c node.
        data_record: The data record of the connection item c node node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")

    space: str
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemC"
    )
    connection_item_a: Optional[list[Union[ConnectionItemAWrite, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemBWrite, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    @field_validator("connection_item_a", "connection_item_b", mode="before")
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
        cache.add(self.as_tuple_id())

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=None if allow_version_increase else self.data_record.existing_version,
            type=as_direct_relation_reference(self.node_type),
            sources=None,
        )
        resources.nodes.append(this_node)

        edge_type = dm.DirectRelationReference("sp_pygen_models", "unidirectional")
        for connection_item_a in self.connection_item_a or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=connection_item_a,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("sp_pygen_models", "unidirectional")
        for connection_item_b in self.connection_item_b or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=connection_item_b,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemCNodeApply(ConnectionItemCNodeWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemCNodeApply:
        warnings.warn(
            "ConnectionItemCNodeApply is deprecated and will be removed in v1.0. Use ConnectionItemCNodeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemCNode.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemCNodeList(DomainModelList[ConnectionItemCNode]):
    """List of connection item c nodes in the read version."""

    _INSTANCE = ConnectionItemCNode

    def as_write(self) -> ConnectionItemCNodeWriteList:
        """Convert these read versions of connection item c node to the writing versions."""
        return ConnectionItemCNodeWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemCNodeWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def connection_item_a(self) -> ConnectionItemAList:
        from ._connection_item_a import ConnectionItemA, ConnectionItemAList

        return ConnectionItemAList(
            [item for items in self.data for item in items.connection_item_a or [] if isinstance(item, ConnectionItemA)]
        )

    @property
    def connection_item_b(self) -> ConnectionItemBList:
        from ._connection_item_b import ConnectionItemB, ConnectionItemBList

        return ConnectionItemBList(
            [item for items in self.data for item in items.connection_item_b or [] if isinstance(item, ConnectionItemB)]
        )


class ConnectionItemCNodeWriteList(DomainModelWriteList[ConnectionItemCNodeWrite]):
    """List of connection item c nodes in the writing version."""

    _INSTANCE = ConnectionItemCNodeWrite

    @property
    def connection_item_a(self) -> ConnectionItemAWriteList:
        from ._connection_item_a import ConnectionItemAWrite, ConnectionItemAWriteList

        return ConnectionItemAWriteList(
            [
                item
                for items in self.data
                for item in items.connection_item_a or []
                if isinstance(item, ConnectionItemAWrite)
            ]
        )

    @property
    def connection_item_b(self) -> ConnectionItemBWriteList:
        from ._connection_item_b import ConnectionItemBWrite, ConnectionItemBWriteList

        return ConnectionItemBWriteList(
            [
                item
                for items in self.data
                for item in items.connection_item_b or []
                if isinstance(item, ConnectionItemBWrite)
            ]
        )


class ConnectionItemCNodeApplyList(ConnectionItemCNodeWriteList): ...


def _create_connection_item_c_node_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ConnectionItemCNodeQuery(NodeQueryCore[T_DomainModelList, ConnectionItemCNodeList]):
    _view_id = ConnectionItemCNode._view_id
    _result_cls = ConnectionItemCNode
    _result_list_cls_end = ConnectionItemCNodeList

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
        from ._connection_item_a import _ConnectionItemAQuery
        from ._connection_item_b import _ConnectionItemBQuery

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

        if _ConnectionItemAQuery not in created_types:
            self.connection_item_a = _ConnectionItemAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="connection_item_a",
            )

        if _ConnectionItemBQuery not in created_types:
            self.connection_item_b = _ConnectionItemBQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="connection_item_b",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_connection_item_c_node(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemCNodeList:
        return self._list(limit=limit)


class ConnectionItemCNodeQuery(_ConnectionItemCNodeQuery[ConnectionItemCNodeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemCNodeList)
