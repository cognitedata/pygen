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
    from omni.data_classes._connection_item_e import (
        ConnectionItemE,
        ConnectionItemEList,
        ConnectionItemEGraphQL,
        ConnectionItemEWrite,
        ConnectionItemEWriteList,
    )


__all__ = [
    "ConnectionItemD",
    "ConnectionItemDWrite",
    "ConnectionItemDList",
    "ConnectionItemDWriteList",
    "ConnectionItemDFields",
    "ConnectionItemDTextFields",
    "ConnectionItemDGraphQL",
]


ConnectionItemDTextFields = Literal["external_id", "name"]
ConnectionItemDFields = Literal["external_id", "name"]

_CONNECTIONITEMD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class ConnectionItemDGraphQL(GraphQLCore):
    """This represents the reading version of connection item d, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item d.
        data_record: The data record of the connection item d node.
        direct_multi: The direct multi field.
        direct_single: The direct single field.
        name: The name field.
        outwards_single: The outwards single field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemD", "1")
    direct_multi: Optional[list[ConnectionItemEGraphQL]] = Field(default=None, repr=False, alias="directMulti")
    direct_single: Optional[ConnectionItemEGraphQL] = Field(default=None, repr=False, alias="directSingle")
    name: Optional[str] = None
    outwards_single: Optional[ConnectionItemEGraphQL] = Field(default=None, repr=False, alias="outwardsSingle")

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

    @field_validator("direct_multi", "direct_single", "outwards_single", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemD:
        """Convert this GraphQL format of connection item d to the reading format."""
        return ConnectionItemD.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionItemDWrite:
        """Convert this GraphQL format of connection item d to the writing format."""
        return ConnectionItemDWrite.model_validate(as_write_args(self))


class ConnectionItemD(DomainModel):
    """This represents the reading version of connection item d.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item d.
        data_record: The data record of the connection item d node.
        direct_multi: The direct multi field.
        direct_single: The direct single field.
        name: The name field.
        outwards_single: The outwards single field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemD", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemD"
    )
    direct_multi: Optional[list[Union[ConnectionItemE, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="directMulti"
    )
    direct_single: Union[ConnectionItemE, str, dm.NodeId, None] = Field(default=None, repr=False, alias="directSingle")
    name: Optional[str] = None
    outwards_single: Union[ConnectionItemE, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="outwardsSingle"
    )

    @field_validator("direct_single", "outwards_single", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("direct_multi", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemDWrite:
        """Convert this read version of connection item d to the writing version."""
        return ConnectionItemDWrite.model_validate(as_write_args(self))


class ConnectionItemDWrite(DomainModelWrite):
    """This represents the writing version of connection item d.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item d.
        data_record: The data record of the connection item d node.
        direct_multi: The direct multi field.
        direct_single: The direct single field.
        name: The name field.
        outwards_single: The outwards single field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "direct_multi",
        "direct_single",
        "name",
    )
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("outwards_single", dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle")),
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "direct_multi",
        "direct_single",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemD", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemD"
    )
    direct_multi: Optional[list[Union[ConnectionItemEWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="directMulti"
    )
    direct_single: Union[ConnectionItemEWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="directSingle"
    )
    name: Optional[str] = None
    outwards_single: Union[ConnectionItemEWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="outwardsSingle"
    )

    @field_validator("direct_multi", "direct_single", "outwards_single", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ConnectionItemDList(DomainModelList[ConnectionItemD]):
    """List of connection item ds in the read version."""

    _INSTANCE = ConnectionItemD

    def as_write(self) -> ConnectionItemDWriteList:
        """Convert these read versions of connection item d to the writing versions."""
        return ConnectionItemDWriteList([node.as_write() for node in self.data])

    @property
    def direct_multi(self) -> ConnectionItemEList:
        from ._connection_item_e import ConnectionItemE, ConnectionItemEList

        return ConnectionItemEList(
            [item for items in self.data for item in items.direct_multi or [] if isinstance(item, ConnectionItemE)]
        )

    @property
    def direct_single(self) -> ConnectionItemEList:
        from ._connection_item_e import ConnectionItemE, ConnectionItemEList

        return ConnectionItemEList(
            [item.direct_single for item in self.data if isinstance(item.direct_single, ConnectionItemE)]
        )

    @property
    def outwards_single(self) -> ConnectionItemEList:
        from ._connection_item_e import ConnectionItemE, ConnectionItemEList

        return ConnectionItemEList(
            [item.outwards_single for item in self.data if isinstance(item.outwards_single, ConnectionItemE)]
        )


class ConnectionItemDWriteList(DomainModelWriteList[ConnectionItemDWrite]):
    """List of connection item ds in the writing version."""

    _INSTANCE = ConnectionItemDWrite

    @property
    def direct_multi(self) -> ConnectionItemEWriteList:
        from ._connection_item_e import ConnectionItemEWrite, ConnectionItemEWriteList

        return ConnectionItemEWriteList(
            [item for items in self.data for item in items.direct_multi or [] if isinstance(item, ConnectionItemEWrite)]
        )

    @property
    def direct_single(self) -> ConnectionItemEWriteList:
        from ._connection_item_e import ConnectionItemEWrite, ConnectionItemEWriteList

        return ConnectionItemEWriteList(
            [item.direct_single for item in self.data if isinstance(item.direct_single, ConnectionItemEWrite)]
        )

    @property
    def outwards_single(self) -> ConnectionItemEWriteList:
        from ._connection_item_e import ConnectionItemEWrite, ConnectionItemEWriteList

        return ConnectionItemEWriteList(
            [item.outwards_single for item in self.data if isinstance(item.outwards_single, ConnectionItemEWrite)]
        )


def _create_connection_item_d_filter(
    view_id: dm.ViewId,
    direct_multi: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    direct_single: (
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
    if isinstance(direct_multi, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(direct_multi):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("directMulti"), value=as_instance_dict_id(direct_multi))
        )
    if (
        direct_multi
        and isinstance(direct_multi, Sequence)
        and not isinstance(direct_multi, str)
        and not is_tuple_id(direct_multi)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directMulti"), values=[as_instance_dict_id(item) for item in direct_multi]
            )
        )
    if isinstance(direct_single, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(direct_single):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("directSingle"), value=as_instance_dict_id(direct_single))
        )
    if (
        direct_single
        and isinstance(direct_single, Sequence)
        and not isinstance(direct_single, str)
        and not is_tuple_id(direct_single)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directSingle"), values=[as_instance_dict_id(item) for item in direct_single]
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


class _ConnectionItemDQuery(NodeQueryCore[T_DomainModelList, ConnectionItemDList]):
    _view_id = ConnectionItemD._view_id
    _result_cls = ConnectionItemD
    _result_list_cls_end = ConnectionItemDList

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
        from ._connection_item_e import _ConnectionItemEQuery

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

        if _ConnectionItemEQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.direct_multi = _ConnectionItemEQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("directMulti"),
                    direction="outwards",
                ),
                connection_name="direct_multi",
                connection_property=ViewPropertyId(self._view_id, "directMulti"),
            )

        if _ConnectionItemEQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.direct_single = _ConnectionItemEQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("directSingle"),
                    direction="outwards",
                ),
                connection_name="direct_single",
                connection_property=ViewPropertyId(self._view_id, "directSingle"),
            )

        if _ConnectionItemEQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.outwards_single = _ConnectionItemEQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="outwards_single",
                connection_property=ViewPropertyId(self._view_id, "outwardsSingle"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.direct_single_filter = DirectRelationFilter(self, self._view_id.as_property_ref("directSingle"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.direct_single_filter,
                self.name,
            ]
        )

    def list_connection_item_d(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemDList:
        return self._list(limit=limit)


class ConnectionItemDQuery(_ConnectionItemDQuery[ConnectionItemDList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemDList)
