from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._connection_item_a import ConnectionItemA, ConnectionItemAGraphQL, ConnectionItemAWrite
    from ._connection_item_b import ConnectionItemB, ConnectionItemBGraphQL, ConnectionItemBWrite


__all__ = [
    "ConnectionItemCNode",
    "ConnectionItemCNodeWrite",
    "ConnectionItemCNodeApply",
    "ConnectionItemCNodeList",
    "ConnectionItemCNodeWriteList",
    "ConnectionItemCNodeApplyList",
    "ConnectionItemCNodeGraphQL",
]


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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemC", "1")
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
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            connection_item_a=[connection_item_a.as_read() for connection_item_a in self.connection_item_a or []],
            connection_item_b=[connection_item_b.as_read() for connection_item_b in self.connection_item_b or []],
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemCNodeWrite:
        """Convert this GraphQL format of connection item c node to the writing format."""
        return ConnectionItemCNodeWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            connection_item_a=[connection_item_a.as_write() for connection_item_a in self.connection_item_a or []],
            connection_item_b=[connection_item_b.as_write() for connection_item_b in self.connection_item_b or []],
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemC", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemC")
    connection_item_a: Optional[list[Union[ConnectionItemA, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemB, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    def as_write(self) -> ConnectionItemCNodeWrite:
        """Convert this read version of connection item c node to the writing version."""
        return ConnectionItemCNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            connection_item_a=[
                connection_item_a.as_write() if isinstance(connection_item_a, DomainModel) else connection_item_a
                for connection_item_a in self.connection_item_a or []
            ],
            connection_item_b=[
                connection_item_b.as_write() if isinstance(connection_item_b, DomainModel) else connection_item_b
                for connection_item_b in self.connection_item_b or []
            ],
        )

    def as_apply(self) -> ConnectionItemCNodeWrite:
        """Convert this read version of connection item c node to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemC", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemC")
    connection_item_a: Optional[list[Union[ConnectionItemAWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Optional[list[Union[ConnectionItemBWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

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
            type=self.node_type,
            sources=None,
        )
        resources.nodes.append(this_node)

        edge_type = dm.DirectRelationReference("pygen-models", "unidirectional")
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

        edge_type = dm.DirectRelationReference("pygen-models", "unidirectional")
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


class ConnectionItemCNodeWriteList(DomainModelWriteList[ConnectionItemCNodeWrite]):
    """List of connection item c nodes in the writing version."""

    _INSTANCE = ConnectionItemCNodeWrite


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
