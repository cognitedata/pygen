from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
)

if TYPE_CHECKING:
    from omni.data_classes._connection_item_b import (
        ConnectionItemB,
        ConnectionItemBList,
        ConnectionItemBGraphQL,
        ConnectionItemBWrite,
        ConnectionItemBWriteList,
    )
    from omni.data_classes._connection_item_c_node import (
        ConnectionItemCNode,
        ConnectionItemCNodeList,
        ConnectionItemCNodeGraphQL,
        ConnectionItemCNodeWrite,
        ConnectionItemCNodeWriteList,
    )


__all__ = [
    "ConnectionItemA",
    "ConnectionItemAWrite",
    "ConnectionItemAApply",
    "ConnectionItemAList",
    "ConnectionItemAWriteList",
    "ConnectionItemAApplyList",
    "ConnectionItemAFields",
    "ConnectionItemATextFields",
    "ConnectionItemAGraphQL",
]


ConnectionItemATextFields = Literal["external_id", "name"]
ConnectionItemAFields = Literal["external_id", "name"]

_CONNECTIONITEMA_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
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
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")
    name: Optional[str] = None
    other_direct: Optional[ConnectionItemCNodeGraphQL] = Field(default=None, repr=False, alias="otherDirect")
    outwards: Optional[list[ConnectionItemBGraphQL]] = Field(default=None, repr=False)
    self_direct: Optional[ConnectionItemAGraphQL] = Field(default=None, repr=False, alias="selfDirect")

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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemA:
        """Convert this GraphQL format of connection item a to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemA(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            other_direct=(
                self.other_direct.as_read() if isinstance(self.other_direct, GraphQLCore) else self.other_direct
            ),
            outwards=[outward.as_read() for outward in self.outwards] if self.outwards is not None else None,
            self_direct=self.self_direct.as_read() if isinstance(self.self_direct, GraphQLCore) else self.self_direct,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemAWrite:
        """Convert this GraphQL format of connection item a to the writing format."""
        return ConnectionItemAWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            other_direct=(
                self.other_direct.as_write() if isinstance(self.other_direct, GraphQLCore) else self.other_direct
            ),
            outwards=[outward.as_write() for outward in self.outwards] if self.outwards is not None else None,
            self_direct=self.self_direct.as_write() if isinstance(self.self_direct, GraphQLCore) else self.self_direct,
        )


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
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemA"
    )
    name: Optional[str] = None
    other_direct: Union[ConnectionItemCNode, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="otherDirect"
    )
    outwards: Optional[list[Union[ConnectionItemB, str, dm.NodeId]]] = Field(default=None, repr=False)
    self_direct: Union[ConnectionItemA, str, dm.NodeId, None] = Field(default=None, repr=False, alias="selfDirect")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemAWrite:
        """Convert this read version of connection item a to the writing version."""
        return ConnectionItemAWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            other_direct=(
                self.other_direct.as_write() if isinstance(self.other_direct, DomainModel) else self.other_direct
            ),
            outwards=(
                [outward.as_write() if isinstance(outward, DomainModel) else outward for outward in self.outwards]
                if self.outwards is not None
                else None
            ),
            self_direct=self.self_direct.as_write() if isinstance(self.self_direct, DomainModel) else self.self_direct,
        )

    def as_apply(self) -> ConnectionItemAWrite:
        """Convert this read version of connection item a to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ConnectionItemA],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._connection_item_b import ConnectionItemB
        from ._connection_item_c_node import ConnectionItemCNode

        for instance in instances.values():
            if (
                isinstance(instance.other_direct, (dm.NodeId, str))
                and (other_direct := nodes_by_id.get(instance.other_direct))
                and isinstance(other_direct, ConnectionItemCNode)
            ):
                instance.other_direct = other_direct
            if (
                isinstance(instance.self_direct, (dm.NodeId, str))
                and (self_direct := nodes_by_id.get(instance.self_direct))
                and isinstance(self_direct, ConnectionItemA)
            ):
                instance.self_direct = self_direct
            if edges := edges_by_source_node.get(instance.as_id()):
                outwards: list[ConnectionItemB | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("sp_pygen_models", "bidirectional") and isinstance(
                        value, (ConnectionItemB, str, dm.NodeId)
                    ):
                        outwards.append(value)

                instance.outwards = outwards or None


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
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemA"
    )
    name: Optional[str] = None
    other_direct: Union[ConnectionItemCNodeWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="otherDirect"
    )
    outwards: Optional[list[Union[ConnectionItemBWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    self_direct: Union[ConnectionItemAWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="selfDirect")

    @field_validator("other_direct", "outwards", "self_direct", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.other_direct is not None:
            properties["otherDirect"] = {
                "space": self.space if isinstance(self.other_direct, str) else self.other_direct.space,
                "externalId": (
                    self.other_direct if isinstance(self.other_direct, str) else self.other_direct.external_id
                ),
            }

        if self.self_direct is not None:
            properties["selfDirect"] = {
                "space": self.space if isinstance(self.self_direct, str) else self.self_direct.space,
                "externalId": self.self_direct if isinstance(self.self_direct, str) else self.self_direct.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("sp_pygen_models", "bidirectional")
        for outward in self.outwards or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=outward,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.other_direct, DomainModelWrite):
            other_resources = self.other_direct._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.self_direct, DomainModelWrite):
            other_resources = self.self_direct._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ConnectionItemAApply(ConnectionItemAWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemAApply:
        warnings.warn(
            "ConnectionItemAApply is deprecated and will be removed in v1.0. Use ConnectionItemAWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemA.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemAList(DomainModelList[ConnectionItemA]):
    """List of connection item as in the read version."""

    _INSTANCE = ConnectionItemA

    def as_write(self) -> ConnectionItemAWriteList:
        """Convert these read versions of connection item a to the writing versions."""
        return ConnectionItemAWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemAWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class ConnectionItemAApplyList(ConnectionItemAWriteList): ...


def _create_connection_item_a_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    other_direct: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    self_direct: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
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
    if isinstance(other_direct, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(other_direct):
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
    if isinstance(self_direct, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(self_direct):
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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ConnectionItemCNodeQuery not in created_types:
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
            )

        if _ConnectionItemBQuery not in created_types:
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
            )

        if _ConnectionItemAQuery not in created_types:
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

    def list_connection_item_a(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemAList:
        return self._list(limit=limit)


class ConnectionItemAQuery(_ConnectionItemAQuery[ConnectionItemAList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemAList)
