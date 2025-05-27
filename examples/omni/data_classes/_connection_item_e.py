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
    DirectRelationFilter,
)

if TYPE_CHECKING:
    from omni.data_classes._connection_edge_a import (
        ConnectionEdgeA,
        ConnectionEdgeAList,
        ConnectionEdgeAGraphQL,
        ConnectionEdgeAWrite,
        ConnectionEdgeAWriteList,
    )
    from omni.data_classes._connection_item_d import (
        ConnectionItemD,
        ConnectionItemDList,
        ConnectionItemDGraphQL,
        ConnectionItemDWrite,
        ConnectionItemDWriteList,
    )


__all__ = [
    "ConnectionItemE",
    "ConnectionItemEWrite",
    "ConnectionItemEList",
    "ConnectionItemEWriteList",
    "ConnectionItemEFields",
    "ConnectionItemETextFields",
    "ConnectionItemEGraphQL",
]


ConnectionItemETextFields = Literal["external_id", "name"]
ConnectionItemEFields = Literal["external_id", "name"]

_CONNECTIONITEME_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class ConnectionItemEGraphQL(GraphQLCore):
    """This represents the reading version of connection item e, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item e.
        data_record: The data record of the connection item e node.
        direct_list_no_source: The direct list no source field.
        direct_no_source: The direct no source field.
        direct_reverse_multi: The direct reverse multi field.
        direct_reverse_single: The direct reverse single field.
        inwards_single: The inwards single field.
        inwards_single_property: The inwards single property field.
        name: The name field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemE", "1")
    direct_list_no_source: Optional[list[dict]] = Field(default=None, alias="directListNoSource")
    direct_no_source: Optional[dict] = Field(default=None, alias="directNoSource")
    direct_reverse_multi: Optional[list[ConnectionItemDGraphQL]] = Field(
        default=None, repr=False, alias="directReverseMulti"
    )
    direct_reverse_single: Optional[ConnectionItemDGraphQL] = Field(
        default=None, repr=False, alias="directReverseSingle"
    )
    inwards_single: Optional[ConnectionItemDGraphQL] = Field(default=None, repr=False, alias="inwardsSingle")
    inwards_single_property: Optional[ConnectionEdgeAGraphQL] = Field(
        default=None, repr=False, alias="inwardsSingleProperty"
    )
    name: Optional[str] = None

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

    @field_validator(
        "direct_list_no_source",
        "direct_no_source",
        "direct_reverse_multi",
        "direct_reverse_single",
        "inwards_single",
        "inwards_single_property",
        mode="before",
    )
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemE:
        """Convert this GraphQL format of connection item e to the reading format."""
        return ConnectionItemE.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionItemEWrite:
        """Convert this GraphQL format of connection item e to the writing format."""
        return ConnectionItemEWrite.model_validate(as_write_args(self))


class ConnectionItemE(DomainModel):
    """This represents the reading version of connection item e.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item e.
        data_record: The data record of the connection item e node.
        direct_list_no_source: The direct list no source field.
        direct_no_source: The direct no source field.
        direct_reverse_multi: The direct reverse multi field.
        direct_reverse_single: The direct reverse single field.
        inwards_single: The inwards single field.
        inwards_single_property: The inwards single property field.
        name: The name field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemE", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemE"
    )
    direct_list_no_source: Optional[list[Union[str, dm.NodeId]]] = Field(default=None, alias="directListNoSource")
    direct_no_source: Union[str, dm.NodeId, None] = Field(default=None, alias="directNoSource")
    direct_reverse_multi: Optional[list[ConnectionItemD]] = Field(default=None, repr=False, alias="directReverseMulti")
    direct_reverse_single: Optional[ConnectionItemD] = Field(default=None, repr=False, alias="directReverseSingle")
    inwards_single: Union[ConnectionItemD, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="inwardsSingle"
    )
    inwards_single_property: Optional[ConnectionEdgeA] = Field(default=None, repr=False, alias="inwardsSingleProperty")
    name: Optional[str] = None

    @field_validator(
        "direct_no_source", "direct_reverse_single", "inwards_single", "inwards_single_property", mode="before"
    )
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("direct_list_no_source", "direct_reverse_multi", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemEWrite:
        """Convert this read version of connection item e to the writing version."""
        return ConnectionItemEWrite.model_validate(as_write_args(self))


class ConnectionItemEWrite(DomainModelWrite):
    """This represents the writing version of connection item e.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item e.
        data_record: The data record of the connection item e node.
        direct_list_no_source: The direct list no source field.
        direct_no_source: The direct no source field.
        inwards_single: The inwards single field.
        inwards_single_property: The inwards single property field.
        name: The name field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "direct_list_no_source",
        "direct_no_source",
        "name",
    )
    _inwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("inwards_single", dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle")),
        ("inwards_single_property", dm.DirectRelationReference("sp_pygen_models", "multiProperty")),
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemE", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemE"
    )
    direct_list_no_source: Optional[list[Union[str, dm.NodeId]]] = Field(default=None, alias="directListNoSource")
    direct_no_source: Union[str, dm.NodeId, None] = Field(default=None, alias="directNoSource")
    inwards_single: Union[ConnectionItemDWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="inwardsSingle"
    )
    inwards_single_property: Optional[ConnectionEdgeAWrite] = Field(
        default=None, repr=False, alias="inwardsSingleProperty"
    )
    name: Optional[str] = None

    @field_validator("inwards_single", "inwards_single_property", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ConnectionItemEList(DomainModelList[ConnectionItemE]):
    """List of connection item es in the read version."""

    _INSTANCE = ConnectionItemE

    def as_write(self) -> ConnectionItemEWriteList:
        """Convert these read versions of connection item e to the writing versions."""
        return ConnectionItemEWriteList([node.as_write() for node in self.data])

    @property
    def direct_reverse_multi(self) -> ConnectionItemDList:
        from ._connection_item_d import ConnectionItemD, ConnectionItemDList

        return ConnectionItemDList(
            [
                item
                for items in self.data
                for item in items.direct_reverse_multi or []
                if isinstance(item, ConnectionItemD)
            ]
        )

    @property
    def direct_reverse_single(self) -> ConnectionItemDList:
        from ._connection_item_d import ConnectionItemD, ConnectionItemDList

        return ConnectionItemDList(
            [
                item.direct_reverse_single
                for item in self.data
                if isinstance(item.direct_reverse_single, ConnectionItemD)
            ]
        )

    @property
    def inwards_single(self) -> ConnectionItemDList:
        from ._connection_item_d import ConnectionItemD, ConnectionItemDList

        return ConnectionItemDList(
            [item.inwards_single for item in self.data if isinstance(item.inwards_single, ConnectionItemD)]
        )

    @property
    def inwards_single_property(self) -> ConnectionEdgeAList:
        from ._connection_edge_a import ConnectionEdgeA, ConnectionEdgeAList

        return ConnectionEdgeAList(
            [
                item.inwards_single_property
                for item in self.data
                if isinstance(item.inwards_single_property, ConnectionEdgeA)
            ]
        )


class ConnectionItemEWriteList(DomainModelWriteList[ConnectionItemEWrite]):
    """List of connection item es in the writing version."""

    _INSTANCE = ConnectionItemEWrite

    @property
    def inwards_single(self) -> ConnectionItemDWriteList:
        from ._connection_item_d import ConnectionItemDWrite, ConnectionItemDWriteList

        return ConnectionItemDWriteList(
            [item.inwards_single for item in self.data if isinstance(item.inwards_single, ConnectionItemDWrite)]
        )

    @property
    def inwards_single_property(self) -> ConnectionEdgeAWriteList:
        from ._connection_edge_a import ConnectionEdgeAWrite, ConnectionEdgeAWriteList

        return ConnectionEdgeAWriteList(
            [
                item.inwards_single_property
                for item in self.data
                if isinstance(item.inwards_single_property, ConnectionEdgeAWrite)
            ]
        )


def _create_connection_item_e_filter(
    view_id: dm.ViewId,
    direct_list_no_source: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    direct_no_source: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(direct_list_no_source, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        direct_list_no_source
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directListNoSource"), value=as_instance_dict_id(direct_list_no_source)
            )
        )
    if (
        direct_list_no_source
        and isinstance(direct_list_no_source, Sequence)
        and not isinstance(direct_list_no_source, str)
        and not is_tuple_id(direct_list_no_source)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directListNoSource"),
                values=[as_instance_dict_id(item) for item in direct_list_no_source],
            )
        )
    if isinstance(direct_no_source, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(direct_no_source):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("directNoSource"), value=as_instance_dict_id(direct_no_source))
        )
    if (
        direct_no_source
        and isinstance(direct_no_source, Sequence)
        and not isinstance(direct_no_source, str)
        and not is_tuple_id(direct_no_source)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directNoSource"),
                values=[as_instance_dict_id(item) for item in direct_no_source],
            )
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ConnectionItemEQuery(NodeQueryCore[T_DomainModelList, ConnectionItemEList]):
    _view_id = ConnectionItemE._view_id
    _result_cls = ConnectionItemE
    _result_list_cls_end = ConnectionItemEList

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
        from ._connection_edge_a import _ConnectionEdgeAQuery
        from ._connection_item_d import _ConnectionItemDQuery
        from ._connection_item_f import _ConnectionItemFQuery

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

        if _ConnectionItemDQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.direct_reverse_multi = _ConnectionItemDQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_models", "ConnectionItemD", "1").as_property_ref("directMulti"),
                    direction="inwards",
                ),
                connection_name="direct_reverse_multi",
                connection_property=ViewPropertyId(self._view_id, "directReverseMulti"),
                connection_type="reverse-list",
            )

        if _ConnectionItemDQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.direct_reverse_single = _ConnectionItemDQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_models", "ConnectionItemD", "1").as_property_ref("directSingle"),
                    direction="inwards",
                ),
                connection_name="direct_reverse_single",
                connection_property=ViewPropertyId(self._view_id, "directReverseSingle"),
            )

        if _ConnectionItemDQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.inwards_single = _ConnectionItemDQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="inwards",
                    chain_to="destination",
                ),
                connection_name="inwards_single",
                connection_property=ViewPropertyId(self._view_id, "inwardsSingle"),
            )

        if _ConnectionEdgeAQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.inwards_single_property = _ConnectionEdgeAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _ConnectionItemFQuery,
                dm.query.EdgeResultSetExpression(
                    direction="inwards",
                    chain_to="destination",
                ),
                connection_name="inwards_single_property",
                connection_property=ViewPropertyId(self._view_id, "inwardsSingleProperty"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.direct_no_source_filter = DirectRelationFilter(self, self._view_id.as_property_ref("directNoSource"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.direct_no_source_filter,
                self.name,
            ]
        )

    def list_connection_item_e(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemEList:
        return self._list(limit=limit)


class ConnectionItemEQuery(_ConnectionItemEQuery[ConnectionItemEList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemEList)
