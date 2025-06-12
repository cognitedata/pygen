from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, TYPE_CHECKING, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field

from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainRelation,
    DomainRelationWrite,
    DomainRelationList,
    DomainRelationWriteList,
    GraphQLCore,
    ResourcesWrite,
    DomainModelList,
    T_DomainList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_read_args,
    as_write_args,
    as_pygen_node_id,
    is_tuple_id,
    EdgeQueryCore,
    NodeQueryCore,
    QueryCore,
    StringFilter,
    ViewPropertyId,
    TimestampFilter,
)
from omni.data_classes._connection_item_f import ConnectionItemFWrite
from omni.data_classes._connection_item_e import ConnectionItemE, ConnectionItemEGraphQL, ConnectionItemEWrite
from omni.data_classes._connection_item_g import ConnectionItemG, ConnectionItemGGraphQL, ConnectionItemGWrite

if TYPE_CHECKING:
    from omni.data_classes._connection_item_e import ConnectionItemE, ConnectionItemEGraphQL, ConnectionItemEWrite
    from omni.data_classes._connection_item_f import ConnectionItemF, ConnectionItemFGraphQL, ConnectionItemFWrite
    from omni.data_classes._connection_item_g import ConnectionItemG, ConnectionItemGGraphQL, ConnectionItemGWrite


__all__ = [
    "ConnectionEdgeA",
    "ConnectionEdgeAWrite",
    "ConnectionEdgeAList",
    "ConnectionEdgeAWriteList",
    "ConnectionEdgeAFields",
    "ConnectionEdgeATextFields",
]


ConnectionEdgeATextFields = Literal["external_id", "name"]
ConnectionEdgeAFields = Literal["external_id", "end_time", "name", "start_time"]
_CONNECTIONEDGEA_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "end_time": "endTime",
    "name": "name",
    "start_time": "startTime",
}


class ConnectionEdgeAGraphQL(GraphQLCore):
    """This represents the reading version of connection edge a, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection edge a.
        data_record: The data record of the connection edge a node.
        end_node: The end node of this edge.
        end_time: The end time field.
        name: The name field.
        start_time: The start time field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionEdgeA", "1")
    end_node: Union[ConnectionItemEGraphQL, ConnectionItemFGraphQL, ConnectionItemGGraphQL, None] = Field(
        None, alias="endNode"
    )
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    name: Optional[str] = None
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")

    def as_read(self) -> ConnectionEdgeA:
        """Convert this GraphQL format of connection edge a to the reading format."""
        return ConnectionEdgeA.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionEdgeAWrite:
        """Convert this GraphQL format of connection edge a to the writing format."""
        return ConnectionEdgeAWrite.model_validate(as_write_args(self))


class ConnectionEdgeA(DomainRelation):
    """This represents the reading version of connection edge a.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection edge a.
        data_record: The data record of the connection edge a edge.
        end_node: The end node of this edge.
        end_time: The end time field.
        name: The name field.
        start_time: The start time field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionEdgeA", "1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[ConnectionItemE, ConnectionItemF, ConnectionItemG, str, dm.NodeId] = Field(alias="endNode")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    name: Optional[str] = None
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")

    def as_write(self) -> ConnectionEdgeAWrite:
        """Convert this read version of connection edge a to the writing version."""
        return ConnectionEdgeAWrite.model_validate(as_write_args(self))


_EXPECTED_START_NODES_BY_END_NODE: dict[type[DomainModelWrite], set[type[DomainModelWrite]]] = {
    ConnectionItemEWrite: {ConnectionItemFWrite},
    ConnectionItemFWrite: {ConnectionItemEWrite},
    ConnectionItemGWrite: {ConnectionItemFWrite},
}


def _validate_end_node(
    start_node: DomainModelWrite,
    end_node: Union[ConnectionItemEWrite, ConnectionItemFWrite, ConnectionItemGWrite, str, dm.NodeId],
) -> None:
    if isinstance(end_node, str | dm.NodeId):
        # Nothing to validate
        return
    if type(end_node) not in _EXPECTED_START_NODES_BY_END_NODE:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Should be one of {[t.__name__ for t in _EXPECTED_START_NODES_BY_END_NODE.keys()]}"
        )
    if type(start_node) not in _EXPECTED_START_NODES_BY_END_NODE[type(end_node)]:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Expected one of: {_EXPECTED_START_NODES_BY_END_NODE[type(end_node)]}"
        )


class ConnectionEdgeAWrite(DomainRelationWrite):
    """This represents the writing version of connection edge a.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection edge a.
        data_record: The data record of the connection edge a edge.
        end_node: The end node of this edge.
        end_time: The end time field.
        name: The name field.
        start_time: The start time field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "end_time",
        "name",
        "start_time",
    )
    _validate_end_node = _validate_end_node

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionEdgeA", "1")
    end_node: Union[ConnectionItemEWrite, ConnectionItemFWrite, ConnectionItemGWrite, str, dm.NodeId] = Field(
        alias="endNode"
    )
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    name: Optional[str] = None
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")


class ConnectionEdgeAList(DomainRelationList[ConnectionEdgeA]):
    """List of connection edge as in the reading version."""

    _INSTANCE = ConnectionEdgeA

    def as_write(self) -> ConnectionEdgeAWriteList:
        """Convert this read version of connection edge a list to the writing version."""
        return ConnectionEdgeAWriteList([edge.as_write() for edge in self])


class ConnectionEdgeAWriteList(DomainRelationWriteList[ConnectionEdgeAWrite]):
    """List of connection edge as in the writing version."""

    _INSTANCE = ConnectionEdgeAWrite


def _create_connection_edge_a_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, str):
        filters.append(
            dm.filters.Equals(["edge", "startNode"], value={"space": start_node_space, "externalId": start_node})
        )
    if start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    (
                        {"space": start_node_space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    if end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    (
                        {"space": space_end_node, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in end_node
                ],
            )
        )
    if min_end_time is not None or max_end_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endTime"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_start_time is not None or max_start_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startTime"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class _ConnectionEdgeAQuery(EdgeQueryCore[T_DomainList, ConnectionEdgeAList]):
    _view_id = ConnectionEdgeA._view_id
    _result_cls = ConnectionEdgeA
    _result_list_cls_end = ConnectionEdgeAList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        end_node_cls: type[NodeQueryCore],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
    ):
        from ._connection_item_e import _ConnectionItemEQuery
        from ._connection_item_f import _ConnectionItemFQuery
        from ._connection_item_g import _ConnectionItemGQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            None,
            connection_name,
            connection_property,
        )
        if end_node_cls not in created_types:
            self.end_node = end_node_cls(
                created_types=created_types.copy(),
                creation_path=self._creation_path,
                client=client,
                result_list_cls=result_list_cls,  # type: ignore[type-var]
                expression=dm.query.NodeResultSetExpression(),
                connection_property=ViewPropertyId(self._view_id, "end_node"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.end_time = TimestampFilter(self, self._view_id.as_property_ref("endTime"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.start_time = TimestampFilter(self, self._view_id.as_property_ref("startTime"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.end_time,
                self.name,
                self.start_time,
            ]
        )
