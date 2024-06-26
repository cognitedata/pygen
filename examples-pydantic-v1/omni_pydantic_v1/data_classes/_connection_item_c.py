from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import validator, root_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
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
    "ConnectionItemC",
    "ConnectionItemCWrite",
    "ConnectionItemCApply",
    "ConnectionItemCList",
    "ConnectionItemCWriteList",
    "ConnectionItemCApplyList",
]


class ConnectionItemCGraphQL(GraphQLCore):
    """This represents the reading version of connection item c, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c.
        data_record: The data record of the connection item c node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    view_id = dm.ViewId("pygen-models", "ConnectionItemC", "1")
    connection_item_a: Optional[list[ConnectionItemAGraphQL]] = Field(default=None, repr=False, alias="connectionItemA")
    connection_item_b: Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False, alias="connectionItemB")

    @root_validator(pre=True)
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @validator("connection_item_a", "connection_item_b", pre=True)
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemC:
        """Convert this GraphQL format of connection item c to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemC(
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

    def as_write(self) -> ConnectionItemCWrite:
        """Convert this GraphQL format of connection item c to the writing format."""
        return ConnectionItemCWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            connection_item_a=[connection_item_a.as_write() for connection_item_a in self.connection_item_a or []],
            connection_item_b=[connection_item_b.as_write() for connection_item_b in self.connection_item_b or []],
        )


class ConnectionItemC(DomainModel):
    """This represents the reading version of connection item c.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c.
        data_record: The data record of the connection item c node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemC")
    connection_item_a: Union[list[ConnectionItemA], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Union[list[ConnectionItemB], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    def as_write(self) -> ConnectionItemCWrite:
        """Convert this read version of connection item c to the writing version."""
        return ConnectionItemCWrite(
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

    def as_apply(self) -> ConnectionItemCWrite:
        """Convert this read version of connection item c to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ConnectionItemCWrite(DomainModelWrite):
    """This represents the writing version of connection item c.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c.
        data_record: The data record of the connection item c node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemC")
    connection_item_a: Union[list[ConnectionItemAWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Union[list[ConnectionItemBWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
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
                view_by_read_class=view_by_read_class,
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
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemCApply(ConnectionItemCWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemCApply:
        warnings.warn(
            "ConnectionItemCApply is deprecated and will be removed in v1.0. Use ConnectionItemCWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemC.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemCList(DomainModelList[ConnectionItemC]):
    """List of connection item cs in the read version."""

    _INSTANCE = ConnectionItemC

    def as_write(self) -> ConnectionItemCWriteList:
        """Convert these read versions of connection item c to the writing versions."""
        return ConnectionItemCWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemCWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ConnectionItemCWriteList(DomainModelWriteList[ConnectionItemCWrite]):
    """List of connection item cs in the writing version."""

    _INSTANCE = ConnectionItemCWrite


class ConnectionItemCApplyList(ConnectionItemCWriteList): ...


def _create_connection_item_c_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
