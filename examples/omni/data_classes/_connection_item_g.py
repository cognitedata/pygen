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


__all__ = [
    "ConnectionItemG",
    "ConnectionItemGWrite",
    "ConnectionItemGList",
    "ConnectionItemGWriteList",
    "ConnectionItemGFields",
    "ConnectionItemGTextFields",
    "ConnectionItemGGraphQL",
]


ConnectionItemGTextFields = Literal["external_id", "name"]
ConnectionItemGFields = Literal["external_id", "name"]

_CONNECTIONITEMG_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class ConnectionItemGGraphQL(GraphQLCore):
    """This represents the reading version of connection item g, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item g.
        data_record: The data record of the connection item g node.
        inwards_multi_property: The inwards multi property field.
        name: The name field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemG", "1")
    inwards_multi_property: Optional[list[ConnectionEdgeAGraphQL]] = Field(
        default=None, repr=False, alias="inwardsMultiProperty"
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

    @field_validator("inwards_multi_property", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemG:
        """Convert this GraphQL format of connection item g to the reading format."""
        return ConnectionItemG.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionItemGWrite:
        """Convert this GraphQL format of connection item g to the writing format."""
        return ConnectionItemGWrite.model_validate(as_write_args(self))


class ConnectionItemG(DomainModel):
    """This represents the reading version of connection item g.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item g.
        data_record: The data record of the connection item g node.
        inwards_multi_property: The inwards multi property field.
        name: The name field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemG", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemG"
    )
    inwards_multi_property: Optional[list[ConnectionEdgeA]] = Field(
        default=None, repr=False, alias="inwardsMultiProperty"
    )
    name: Optional[str] = None

    @field_validator("inwards_multi_property", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemGWrite:
        """Convert this read version of connection item g to the writing version."""
        return ConnectionItemGWrite.model_validate(as_write_args(self))


class ConnectionItemGWrite(DomainModelWrite):
    """This represents the writing version of connection item g.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item g.
        data_record: The data record of the connection item g node.
        inwards_multi_property: The inwards multi property field.
        name: The name field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("name",)
    _inwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("inwards_multi_property", dm.DirectRelationReference("sp_pygen_models", "multiProperty")),
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemG", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemG"
    )
    inwards_multi_property: Optional[list[ConnectionEdgeAWrite]] = Field(
        default=None, repr=False, alias="inwardsMultiProperty"
    )
    name: Optional[str] = None

    @field_validator("inwards_multi_property", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ConnectionItemGList(DomainModelList[ConnectionItemG]):
    """List of connection item gs in the read version."""

    _INSTANCE = ConnectionItemG

    def as_write(self) -> ConnectionItemGWriteList:
        """Convert these read versions of connection item g to the writing versions."""
        return ConnectionItemGWriteList([node.as_write() for node in self.data])

    @property
    def inwards_multi_property(self) -> ConnectionEdgeAList:
        from ._connection_edge_a import ConnectionEdgeA, ConnectionEdgeAList

        return ConnectionEdgeAList(
            [
                item
                for items in self.data
                for item in items.inwards_multi_property or []
                if isinstance(item, ConnectionEdgeA)
            ]
        )


class ConnectionItemGWriteList(DomainModelWriteList[ConnectionItemGWrite]):
    """List of connection item gs in the writing version."""

    _INSTANCE = ConnectionItemGWrite

    @property
    def inwards_multi_property(self) -> ConnectionEdgeAWriteList:
        from ._connection_edge_a import ConnectionEdgeAWrite, ConnectionEdgeAWriteList

        return ConnectionEdgeAWriteList(
            [
                item
                for items in self.data
                for item in items.inwards_multi_property or []
                if isinstance(item, ConnectionEdgeAWrite)
            ]
        )


def _create_connection_item_g_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ConnectionItemGQuery(NodeQueryCore[T_DomainModelList, ConnectionItemGList]):
    _view_id = ConnectionItemG._view_id
    _result_cls = ConnectionItemG
    _result_list_cls_end = ConnectionItemGList

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

        if _ConnectionEdgeAQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.inwards_multi_property = _ConnectionEdgeAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _ConnectionItemFQuery,
                dm.query.EdgeResultSetExpression(
                    direction="inwards",
                    chain_to="destination",
                ),
                connection_name="inwards_multi_property",
                connection_property=ViewPropertyId(self._view_id, "inwardsMultiProperty"),
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

    def list_connection_item_g(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemGList:
        return self._list(limit=limit)


class ConnectionItemGQuery(_ConnectionItemGQuery[ConnectionItemGList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemGList)
