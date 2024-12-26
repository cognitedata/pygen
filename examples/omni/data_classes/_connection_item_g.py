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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
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
    "ConnectionItemGApply",
    "ConnectionItemGList",
    "ConnectionItemGWriteList",
    "ConnectionItemGApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemG:
        """Convert this GraphQL format of connection item g to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemG(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            inwards_multi_property=(
                [inwards_multi_property.as_read() for inwards_multi_property in self.inwards_multi_property]
                if self.inwards_multi_property is not None
                else None
            ),
            name=self.name,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemGWrite:
        """Convert this GraphQL format of connection item g to the writing format."""
        return ConnectionItemGWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            inwards_multi_property=(
                [inwards_multi_property.as_write() for inwards_multi_property in self.inwards_multi_property]
                if self.inwards_multi_property is not None
                else None
            ),
            name=self.name,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemGWrite:
        """Convert this read version of connection item g to the writing version."""
        return ConnectionItemGWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            inwards_multi_property=(
                [inwards_multi_property.as_write() for inwards_multi_property in self.inwards_multi_property]
                if self.inwards_multi_property is not None
                else None
            ),
            name=self.name,
        )

    def as_apply(self) -> ConnectionItemGWrite:
        """Convert this read version of connection item g to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ConnectionItemG],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._connection_edge_a import ConnectionEdgeA

        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                inwards_multi_property: list[ConnectionEdgeA] = []
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

                    if edge_type == dm.DirectRelationReference("sp_pygen_models", "multiProperty") and isinstance(
                        value, ConnectionEdgeA
                    ):
                        inwards_multi_property.append(value)
                        if end_node := nodes_by_id.get(as_pygen_node_id(value.end_node)):
                            value.end_node = end_node  # type: ignore[assignment]

                instance.inwards_multi_property = inwards_multi_property


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

        for inwards_multi_property in self.inwards_multi_property or []:
            if isinstance(inwards_multi_property, DomainRelationWrite):
                other_resources = inwards_multi_property._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("sp_pygen_models", "multiProperty"),
                )
                resources.extend(other_resources)

        return resources


class ConnectionItemGApply(ConnectionItemGWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemGApply:
        warnings.warn(
            "ConnectionItemGApply is deprecated and will be removed in v1.0. "
            "Use ConnectionItemGWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemG.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemGList(DomainModelList[ConnectionItemG]):
    """List of connection item gs in the read version."""

    _INSTANCE = ConnectionItemG

    def as_write(self) -> ConnectionItemGWriteList:
        """Convert these read versions of connection item g to the writing versions."""
        return ConnectionItemGWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemGWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class ConnectionItemGApplyList(ConnectionItemGWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ConnectionEdgeAQuery not in created_types:
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
