from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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


__all__ = [
    "ConnectionItemB",
    "ConnectionItemBWrite",
    "ConnectionItemBApply",
    "ConnectionItemBList",
    "ConnectionItemBWriteList",
    "ConnectionItemBApplyList",
    "ConnectionItemBFields",
    "ConnectionItemBTextFields",
    "ConnectionItemBGraphQL",
]


ConnectionItemBTextFields = Literal["external_id", "name"]
ConnectionItemBFields = Literal["external_id", "name"]

_CONNECTIONITEMB_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class ConnectionItemBGraphQL(GraphQLCore):
    """This represents the reading version of connection item b, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item b.
        data_record: The data record of the connection item b node.
        inwards: The inward field.
        name: The name field.
        self_edge: The self edge field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemB", "1")
    inwards: Optional[list[ConnectionItemAGraphQL]] = Field(default=None, repr=False)
    name: Optional[str] = None
    self_edge: Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False, alias="selfEdge")

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

    @field_validator("inwards", "self_edge", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemB:
        """Convert this GraphQL format of connection item b to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemB(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            inwards=[inward.as_read() for inward in self.inwards] if self.inwards is not None else None,
            name=self.name,
            self_edge=[self_edge.as_read() for self_edge in self.self_edge] if self.self_edge is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemBWrite:
        """Convert this GraphQL format of connection item b to the writing format."""
        return ConnectionItemBWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            inwards=[inward.as_write() for inward in self.inwards] if self.inwards is not None else None,
            name=self.name,
            self_edge=[self_edge.as_write() for self_edge in self.self_edge] if self.self_edge is not None else None,
        )


class ConnectionItemB(DomainModel):
    """This represents the reading version of connection item b.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item b.
        data_record: The data record of the connection item b node.
        inwards: The inward field.
        name: The name field.
        self_edge: The self edge field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemB", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemB"
    )
    inwards: Optional[list[Union[ConnectionItemA, str, dm.NodeId]]] = Field(default=None, repr=False)
    name: Optional[str] = None
    self_edge: Optional[list[Union[ConnectionItemB, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="selfEdge"
    )

    @field_validator("inwards", "self_edge", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemBWrite:
        """Convert this read version of connection item b to the writing version."""
        return ConnectionItemBWrite.model_validate(as_write_args(self))

    def as_apply(self) -> ConnectionItemBWrite:
        """Convert this read version of connection item b to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ConnectionItemBWrite(DomainModelWrite):
    """This represents the writing version of connection item b.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item b.
        data_record: The data record of the connection item b node.
        inwards: The inward field.
        name: The name field.
        self_edge: The self edge field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("name",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("self_edge", dm.DirectRelationReference("sp_pygen_models", "reflexive")),
    )
    _inwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("inwards", dm.DirectRelationReference("sp_pygen_models", "bidirectional")),
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemB", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemB"
    )
    inwards: Optional[list[Union[ConnectionItemAWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    name: Optional[str] = None
    self_edge: Optional[list[Union[ConnectionItemBWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="selfEdge"
    )

    @field_validator("inwards", "self_edge", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ConnectionItemBApply(ConnectionItemBWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemBApply:
        warnings.warn(
            "ConnectionItemBApply is deprecated and will be removed in v1.0. "
            "Use ConnectionItemBWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemB.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemBList(DomainModelList[ConnectionItemB]):
    """List of connection item bs in the read version."""

    _INSTANCE = ConnectionItemB

    def as_write(self) -> ConnectionItemBWriteList:
        """Convert these read versions of connection item b to the writing versions."""
        return ConnectionItemBWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemBWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def inwards(self) -> ConnectionItemAList:
        from ._connection_item_a import ConnectionItemA, ConnectionItemAList

        return ConnectionItemAList(
            [item for items in self.data for item in items.inwards or [] if isinstance(item, ConnectionItemA)]
        )

    @property
    def self_edge(self) -> ConnectionItemBList:
        return ConnectionItemBList(
            [item for items in self.data for item in items.self_edge or [] if isinstance(item, ConnectionItemB)]
        )


class ConnectionItemBWriteList(DomainModelWriteList[ConnectionItemBWrite]):
    """List of connection item bs in the writing version."""

    _INSTANCE = ConnectionItemBWrite

    @property
    def inwards(self) -> ConnectionItemAWriteList:
        from ._connection_item_a import ConnectionItemAWrite, ConnectionItemAWriteList

        return ConnectionItemAWriteList(
            [item for items in self.data for item in items.inwards or [] if isinstance(item, ConnectionItemAWrite)]
        )

    @property
    def self_edge(self) -> ConnectionItemBWriteList:
        return ConnectionItemBWriteList(
            [item for items in self.data for item in items.self_edge or [] if isinstance(item, ConnectionItemBWrite)]
        )


class ConnectionItemBApplyList(ConnectionItemBWriteList): ...


def _create_connection_item_b_filter(
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


class _ConnectionItemBQuery(NodeQueryCore[T_DomainModelList, ConnectionItemBList]):
    _view_id = ConnectionItemB._view_id
    _result_cls = ConnectionItemB
    _result_list_cls_end = ConnectionItemBList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._connection_item_a import _ConnectionItemAQuery

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

        if _ConnectionItemAQuery not in created_types:
            self.inwards = _ConnectionItemAQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="inwards",
                    chain_to="destination",
                ),
                connection_name="inwards",
                connection_property=ViewPropertyId(self._view_id, "inwards"),
            )

        if _ConnectionItemBQuery not in created_types:
            self.self_edge = _ConnectionItemBQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="self_edge",
                connection_property=ViewPropertyId(self._view_id, "selfEdge"),
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

    def list_connection_item_b(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemBList:
        return self._list(limit=limit)


class ConnectionItemBQuery(_ConnectionItemBQuery[ConnectionItemBList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemBList)
