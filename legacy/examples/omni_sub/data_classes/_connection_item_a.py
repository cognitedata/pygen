from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from omni_sub.config import global_config
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
    DirectRelationFilter,
)

if TYPE_CHECKING:
    from omni_sub.data_classes._connection_item_b import (
        ConnectionItemB,
        ConnectionItemBList,
        ConnectionItemBGraphQL,
        ConnectionItemBWrite,
        ConnectionItemBWriteList,
    )
    from omni_sub.data_classes._connection_item_c_node import (
        ConnectionItemCNode,
        ConnectionItemCNodeList,
        ConnectionItemCNodeGraphQL,
        ConnectionItemCNodeWrite,
        ConnectionItemCNodeWriteList,
    )


__all__ = [
    "ConnectionItemA",
    "ConnectionItemAWrite",
    "ConnectionItemAList",
    "ConnectionItemAWriteList",
    "ConnectionItemAFields",
    "ConnectionItemATextFields",
    "ConnectionItemAGraphQL",
]


ConnectionItemATextFields = Literal["external_id", "name", "properties_"]
ConnectionItemAFields = Literal["external_id", "name", "properties_"]

_CONNECTIONITEMA_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "properties_": "properties",
}


class ConnectionItemAGraphQL(GraphQLCore):
    """This represents the reading version of connection item a, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item a.
        data_record: The data record of the connection item a node.
        name: The name field.
        other_direct: The other direct field.
        outwards: The outward field.
        self_direct: The self direct field.
        properties_: The property field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")
    name: Optional[str] = None
    other_direct: Optional[ConnectionItemCNodeGraphQL] = Field(default=None, repr=False, alias="otherDirect")
    outwards: Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False)
    self_direct: Optional[ConnectionItemAGraphQL] = Field(default=None, repr=False, alias="selfDirect")
    properties_: Optional[str] = Field(None, alias="properties")

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

    @field_validator("other_direct", "outwards", "self_direct", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemA:
        """Convert this GraphQL format of connection item a to the reading format."""
        return ConnectionItemA.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionItemAWrite:
        """Convert this GraphQL format of connection item a to the writing format."""
        return ConnectionItemAWrite.model_validate(as_write_args(self))


class ConnectionItemA(DomainModel):
    """This represents the reading version of connection item a.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item a.
        data_record: The data record of the connection item a node.
        name: The name field.
        other_direct: The other direct field.
        outwards: The outward field.
        self_direct: The self direct field.
        properties_: The property field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")

    space: str
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemA"
    )
    name: Optional[str] = None
    other_direct: Union[ConnectionItemCNode, dm.NodeId, None] = Field(default=None, repr=False, alias="otherDirect")
    outwards: Optional[list[Union[ConnectionItemB, dm.NodeId]]] = Field(default=None, repr=False)
    self_direct: Union[ConnectionItemA, dm.NodeId, None] = Field(default=None, repr=False, alias="selfDirect")
    properties_: Optional[str] = Field(None, alias="properties")

    @field_validator("other_direct", "self_direct", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("outwards", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemAWrite:
        """Convert this read version of connection item a to the writing version."""
        return ConnectionItemAWrite.model_validate(as_write_args(self))


class ConnectionItemAWrite(DomainModelWrite):
    """This represents the writing version of connection item a.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item a.
        data_record: The data record of the connection item a node.
        name: The name field.
        other_direct: The other direct field.
        outwards: The outward field.
        self_direct: The self direct field.
        properties_: The property field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "name",
        "other_direct",
        "properties_",
        "self_direct",
    )
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("outwards", dm.DirectRelationReference("sp_pygen_models", "bidirectional")),
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "other_direct",
        "self_direct",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")

    space: str
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemA"
    )
    name: Optional[str] = None
    other_direct: Union[ConnectionItemCNodeWrite, dm.NodeId, None] = Field(
        default=None, repr=False, alias="otherDirect"
    )
    outwards: Optional[list[Union[ConnectionItemBWrite, dm.NodeId]]] = Field(default=None, repr=False)
    self_direct: Union[ConnectionItemAWrite, dm.NodeId, None] = Field(default=None, repr=False, alias="selfDirect")
    properties_: Optional[str] = Field(None, alias="properties")

    @field_validator("other_direct", "outwards", "self_direct", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ConnectionItemAList(DomainModelList[ConnectionItemA]):
    """List of connection item as in the read version."""

    _INSTANCE = ConnectionItemA

    def as_write(self) -> ConnectionItemAWriteList:
        """Convert these read versions of connection item a to the writing versions."""
        return ConnectionItemAWriteList([node.as_write() for node in self.data])

    @property
    def other_direct(self) -> ConnectionItemCNodeList:
        from ._connection_item_c_node import ConnectionItemCNode, ConnectionItemCNodeList

        return ConnectionItemCNodeList(
            [item.other_direct for item in self.data if isinstance(item.other_direct, ConnectionItemCNode)]
        )

    @property
    def outwards(self) -> ConnectionItemBList:
        from ._connection_item_b import ConnectionItemB, ConnectionItemBList

        return ConnectionItemBList(
            [item for items in self.data for item in items.outwards or [] if isinstance(item, ConnectionItemB)]
        )

    @property
    def self_direct(self) -> ConnectionItemAList:
        return ConnectionItemAList(
            [item.self_direct for item in self.data if isinstance(item.self_direct, ConnectionItemA)]
        )


class ConnectionItemAWriteList(DomainModelWriteList[ConnectionItemAWrite]):
    """List of connection item as in the writing version."""

    _INSTANCE = ConnectionItemAWrite

    @property
    def other_direct(self) -> ConnectionItemCNodeWriteList:
        from ._connection_item_c_node import ConnectionItemCNodeWrite, ConnectionItemCNodeWriteList

        return ConnectionItemCNodeWriteList(
            [item.other_direct for item in self.data if isinstance(item.other_direct, ConnectionItemCNodeWrite)]
        )

    @property
    def outwards(self) -> ConnectionItemBWriteList:
        from ._connection_item_b import ConnectionItemBWrite, ConnectionItemBWriteList

        return ConnectionItemBWriteList(
            [item for items in self.data for item in items.outwards or [] if isinstance(item, ConnectionItemBWrite)]
        )

    @property
    def self_direct(self) -> ConnectionItemAWriteList:
        return ConnectionItemAWriteList(
            [item.self_direct for item in self.data if isinstance(item.self_direct, ConnectionItemAWrite)]
        )


def _create_connection_item_a_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    other_direct: (
        tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    self_direct: (
        tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    properties_: str | list[str] | None = None,
    properties_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(other_direct, dm.NodeId | dm.DirectRelationReference) or is_tuple_id(other_direct):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("otherDirect"), value=as_instance_dict_id(other_direct))
        )
    if (
        other_direct
        and isinstance(other_direct, Sequence)
        and not isinstance(other_direct, str)
        and not is_tuple_id(other_direct)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("otherDirect"), values=[as_instance_dict_id(item) for item in other_direct]
            )
        )
    if isinstance(self_direct, dm.NodeId | dm.DirectRelationReference) or is_tuple_id(self_direct):
        filters.append(dm.filters.Equals(view_id.as_property_ref("selfDirect"), value=as_instance_dict_id(self_direct)))
    if (
        self_direct
        and isinstance(self_direct, Sequence)
        and not isinstance(self_direct, str)
        and not is_tuple_id(self_direct)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("selfDirect"), values=[as_instance_dict_id(item) for item in self_direct]
            )
        )
    if isinstance(properties_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("properties"), value=properties_))
    if properties_ and isinstance(properties_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("properties"), values=properties_))
    if properties_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("properties"), value=properties_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ConnectionItemAQuery(NodeQueryCore[T_DomainModelList, ConnectionItemAList]):
    _view_id = ConnectionItemA._view_id
    _result_cls = ConnectionItemA
    _result_list_cls_end = ConnectionItemAList

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
        from ._connection_item_b import _ConnectionItemBQuery
        from ._connection_item_c_node import _ConnectionItemCNodeQuery

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

        if _ConnectionItemCNodeQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.other_direct = _ConnectionItemCNodeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("otherDirect"),
                    direction="outwards",
                ),
                connection_name="other_direct",
                connection_property=ViewPropertyId(self._view_id, "otherDirect"),
            )

        if _ConnectionItemBQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.outwards = _ConnectionItemBQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="outwards",
                connection_property=ViewPropertyId(self._view_id, "outwards"),
            )

        if _ConnectionItemAQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.self_direct = _ConnectionItemAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("selfDirect"),
                    direction="outwards",
                ),
                connection_name="self_direct",
                connection_property=ViewPropertyId(self._view_id, "selfDirect"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.other_direct_filter = DirectRelationFilter(self, self._view_id.as_property_ref("otherDirect"))
        self.self_direct_filter = DirectRelationFilter(self, self._view_id.as_property_ref("selfDirect"))
        self.properties_ = StringFilter(self, self._view_id.as_property_ref("properties"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.name,
                self.other_direct_filter,
                self.self_direct_filter,
                self.properties_,
            ]
        )

    def list_connection_item_a(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemAList:
        return self._list(limit=limit)


class ConnectionItemAQuery(_ConnectionItemAQuery[ConnectionItemAList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemAList)
