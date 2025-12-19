from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from omni.config import global_config
from omni.data_classes._core import (
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
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
)

if TYPE_CHECKING:
    from omni.data_classes._connection_item_a import (
        ConnectionItemA,
        ConnectionItemAList,
        ConnectionItemAGraphQL,
        ConnectionItemAWrite,
        ConnectionItemAWriteList,
    )
    from omni.data_classes._connection_item_b import (
        ConnectionItemB,
        ConnectionItemBList,
        ConnectionItemBGraphQL,
        ConnectionItemBWrite,
        ConnectionItemBWriteList,
    )


__all__ = [
    "ConnectionItemCNode",
    "ConnectionItemCNodeWrite",
    "ConnectionItemCNodeList",
    "ConnectionItemCNodeWriteList",
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

    def as_read(self) -> ConnectionItemCNode:
        """Convert this GraphQL format of connection item c node to the reading format."""
        return ConnectionItemCNode.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionItemCNodeWrite:
        """Convert this GraphQL format of connection item c node to the writing format."""
        return ConnectionItemCNodeWrite.model_validate(as_write_args(self))


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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemC"
    )
    connection_item_a: Optional[list[Union[ConnectionItemA, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemB, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    @field_validator("connection_item_a", "connection_item_b", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemCNodeWrite:
        """Convert this read version of connection item c node to the writing version."""
        return ConnectionItemCNodeWrite.model_validate(as_write_args(self))


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

    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("connection_item_a", dm.DirectRelationReference("sp_pygen_models", "unidirectional")),
        ("connection_item_b", dm.DirectRelationReference("sp_pygen_models", "unidirectional")),
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemC"
    )
    connection_item_a: Optional[list[Union[ConnectionItemAWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemBWrite, str, dm.NodeId]]] = Field(
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


class ConnectionItemCNodeList(DomainModelList[ConnectionItemCNode]):
    """List of connection item c nodes in the read version."""

    _INSTANCE = ConnectionItemCNode

    def as_write(self) -> ConnectionItemCNodeWriteList:
        """Convert these read versions of connection item c node to the writing versions."""
        return ConnectionItemCNodeWriteList([node.as_write() for node in self.data])

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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _ConnectionItemAQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "connectionItemA"),
            )

        if _ConnectionItemBQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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

    def list_connection_item_c_node(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemCNodeList:
        return self._list(limit=limit)


class ConnectionItemCNodeQuery(_ConnectionItemCNodeQuery[ConnectionItemCNodeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemCNodeList)
