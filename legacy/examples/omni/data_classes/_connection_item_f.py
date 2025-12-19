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
    "ConnectionItemF",
    "ConnectionItemFWrite",
    "ConnectionItemFList",
    "ConnectionItemFWriteList",
    "ConnectionItemFFields",
    "ConnectionItemFTextFields",
    "ConnectionItemFGraphQL",
]


ConnectionItemFTextFields = Literal["external_id", "name"]
ConnectionItemFFields = Literal["external_id", "name"]

_CONNECTIONITEMF_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class ConnectionItemFGraphQL(GraphQLCore):
    """This represents the reading version of connection item f, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        data_record: The data record of the connection item f node.
        direct_list: The direct list field.
        name: The name field.
        outwards_multi: The outwards multi field.
        outwards_single: The outwards single field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemF", "1")
    direct_list: Optional[list[ConnectionItemDGraphQL]] = Field(default=None, repr=False, alias="directList")
    name: Optional[str] = None
    outwards_multi: Optional[list[ConnectionEdgeAGraphQL]] = Field(default=None, repr=False, alias="outwardsMulti")
    outwards_single: Optional[ConnectionEdgeAGraphQL] = Field(default=None, repr=False, alias="outwardsSingle")

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

    @field_validator("direct_list", "outwards_multi", "outwards_single", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemF:
        """Convert this GraphQL format of connection item f to the reading format."""
        return ConnectionItemF.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionItemFWrite:
        """Convert this GraphQL format of connection item f to the writing format."""
        return ConnectionItemFWrite.model_validate(as_write_args(self))


class ConnectionItemF(DomainModel):
    """This represents the reading version of connection item f.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        data_record: The data record of the connection item f node.
        direct_list: The direct list field.
        name: The name field.
        outwards_multi: The outwards multi field.
        outwards_single: The outwards single field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemF", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemF"
    )
    direct_list: Optional[list[Union[ConnectionItemD, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="directList"
    )
    name: Optional[str] = None
    outwards_multi: Optional[list[ConnectionEdgeA]] = Field(default=None, repr=False, alias="outwardsMulti")
    outwards_single: Optional[ConnectionEdgeA] = Field(default=None, repr=False, alias="outwardsSingle")

    @field_validator("outwards_single", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("direct_list", "outwards_multi", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemFWrite:
        """Convert this read version of connection item f to the writing version."""
        return ConnectionItemFWrite.model_validate(as_write_args(self))


class ConnectionItemFWrite(DomainModelWrite):
    """This represents the writing version of connection item f.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        data_record: The data record of the connection item f node.
        direct_list: The direct list field.
        name: The name field.
        outwards_multi: The outwards multi field.
        outwards_single: The outwards single field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "direct_list",
        "name",
    )
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("outwards_multi", dm.DirectRelationReference("sp_pygen_models", "multiProperty")),
        ("outwards_single", dm.DirectRelationReference("sp_pygen_models", "singleProperty")),
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("direct_list",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemF", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemF"
    )
    direct_list: Optional[list[Union[ConnectionItemDWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="directList"
    )
    name: Optional[str] = None
    outwards_multi: Optional[list[ConnectionEdgeAWrite]] = Field(default=None, repr=False, alias="outwardsMulti")
    outwards_single: Optional[ConnectionEdgeAWrite] = Field(default=None, repr=False, alias="outwardsSingle")

    @field_validator("direct_list", "outwards_multi", "outwards_single", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ConnectionItemFList(DomainModelList[ConnectionItemF]):
    """List of connection item fs in the read version."""

    _INSTANCE = ConnectionItemF

    def as_write(self) -> ConnectionItemFWriteList:
        """Convert these read versions of connection item f to the writing versions."""
        return ConnectionItemFWriteList([node.as_write() for node in self.data])

    @property
    def direct_list(self) -> ConnectionItemDList:
        from ._connection_item_d import ConnectionItemD, ConnectionItemDList

        return ConnectionItemDList(
            [item for items in self.data for item in items.direct_list or [] if isinstance(item, ConnectionItemD)]
        )

    @property
    def outwards_multi(self) -> ConnectionEdgeAList:
        from ._connection_edge_a import ConnectionEdgeA, ConnectionEdgeAList

        return ConnectionEdgeAList(
            [item for items in self.data for item in items.outwards_multi or [] if isinstance(item, ConnectionEdgeA)]
        )

    @property
    def outwards_single(self) -> ConnectionEdgeAList:
        from ._connection_edge_a import ConnectionEdgeA, ConnectionEdgeAList

        return ConnectionEdgeAList(
            [item.outwards_single for item in self.data if isinstance(item.outwards_single, ConnectionEdgeA)]
        )


class ConnectionItemFWriteList(DomainModelWriteList[ConnectionItemFWrite]):
    """List of connection item fs in the writing version."""

    _INSTANCE = ConnectionItemFWrite

    @property
    def direct_list(self) -> ConnectionItemDWriteList:
        from ._connection_item_d import ConnectionItemDWrite, ConnectionItemDWriteList

        return ConnectionItemDWriteList(
            [item for items in self.data for item in items.direct_list or [] if isinstance(item, ConnectionItemDWrite)]
        )

    @property
    def outwards_multi(self) -> ConnectionEdgeAWriteList:
        from ._connection_edge_a import ConnectionEdgeAWrite, ConnectionEdgeAWriteList

        return ConnectionEdgeAWriteList(
            [
                item
                for items in self.data
                for item in items.outwards_multi or []
                if isinstance(item, ConnectionEdgeAWrite)
            ]
        )

    @property
    def outwards_single(self) -> ConnectionEdgeAWriteList:
        from ._connection_edge_a import ConnectionEdgeAWrite, ConnectionEdgeAWriteList

        return ConnectionEdgeAWriteList(
            [item.outwards_single for item in self.data if isinstance(item.outwards_single, ConnectionEdgeAWrite)]
        )


def _create_connection_item_f_filter(
    view_id: dm.ViewId,
    direct_list: (
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
    if isinstance(direct_list, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(direct_list):
        filters.append(dm.filters.Equals(view_id.as_property_ref("directList"), value=as_instance_dict_id(direct_list)))
    if (
        direct_list
        and isinstance(direct_list, Sequence)
        and not isinstance(direct_list, str)
        and not is_tuple_id(direct_list)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directList"), values=[as_instance_dict_id(item) for item in direct_list]
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


class _ConnectionItemFQuery(NodeQueryCore[T_DomainModelList, ConnectionItemFList]):
    _view_id = ConnectionItemF._view_id
    _result_cls = ConnectionItemF
    _result_list_cls_end = ConnectionItemFList

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
        from ._connection_item_e import _ConnectionItemEQuery
        from ._connection_item_g import _ConnectionItemGQuery

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
            self.direct_list = _ConnectionItemDQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("directList"),
                    direction="outwards",
                ),
                connection_name="direct_list",
                connection_property=ViewPropertyId(self._view_id, "directList"),
            )

        if _ConnectionEdgeAQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.outwards_multi = _ConnectionEdgeAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _ConnectionItemGQuery,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="outwards_multi",
                connection_property=ViewPropertyId(self._view_id, "outwardsMulti"),
            )

        if _ConnectionEdgeAQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.outwards_single = _ConnectionEdgeAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _ConnectionItemEQuery,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="outwards_single",
                connection_property=ViewPropertyId(self._view_id, "outwardsSingle"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.name,
            ]
        )

    def list_connection_item_f(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemFList:
        return self._list(limit=limit)


class ConnectionItemFQuery(_ConnectionItemFQuery[ConnectionItemFList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemFList)
