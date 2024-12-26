from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field, field_validator, model_validator

from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelList,
    DomainModelWrite,
    DomainModelWriteList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainModelList,
    are_nodes_equal,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    is_tuple_id,
    select_best_node,
)

if TYPE_CHECKING:
    from omni.data_classes._connection_edge_a import (
        ConnectionEdgeA,
        ConnectionEdgeAGraphQL,
        ConnectionEdgeAList,
        ConnectionEdgeAWrite,
        ConnectionEdgeAWriteList,
    )
    from omni.data_classes._connection_item_d import (
        ConnectionItemD,
        ConnectionItemDGraphQL,
        ConnectionItemDList,
        ConnectionItemDWrite,
        ConnectionItemDWriteList,
    )


__all__ = [
    "ConnectionItemE",
    "ConnectionItemEWrite",
    "ConnectionItemEApply",
    "ConnectionItemEList",
    "ConnectionItemEWriteList",
    "ConnectionItemEApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemE:
        """Convert this GraphQL format of connection item e to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemE(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            direct_list_no_source=(
                [dm.NodeId.load(direct_list_no_source) for direct_list_no_source in self.direct_list_no_source]
                if self.direct_list_no_source is not None
                else None
            ),
            direct_no_source=self.direct_no_source,
            direct_reverse_multi=(
                [direct_reverse_multi.as_read() for direct_reverse_multi in self.direct_reverse_multi]
                if self.direct_reverse_multi is not None
                else None
            ),
            direct_reverse_single=(
                self.direct_reverse_single.as_read()
                if isinstance(self.direct_reverse_single, GraphQLCore)
                else self.direct_reverse_single
            ),
            inwards_single=(
                self.inwards_single.as_read() if isinstance(self.inwards_single, GraphQLCore) else self.inwards_single
            ),
            inwards_single_property=(
                self.inwards_single_property.as_read()
                if isinstance(self.inwards_single_property, GraphQLCore)
                else self.inwards_single_property
            ),
            name=self.name,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemEWrite:
        """Convert this GraphQL format of connection item e to the writing format."""
        return ConnectionItemEWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            direct_list_no_source=(
                [dm.NodeId.load(direct_list_no_source) for direct_list_no_source in self.direct_list_no_source]
                if self.direct_list_no_source is not None
                else None
            ),
            direct_no_source=self.direct_no_source,
            inwards_single=(
                self.inwards_single.as_write() if isinstance(self.inwards_single, GraphQLCore) else self.inwards_single
            ),
            inwards_single_property=(
                self.inwards_single_property.as_write()
                if isinstance(self.inwards_single_property, GraphQLCore)
                else self.inwards_single_property
            ),
            name=self.name,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemEWrite:
        """Convert this read version of connection item e to the writing version."""
        return ConnectionItemEWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            direct_list_no_source=(
                [direct_list_no_source for direct_list_no_source in self.direct_list_no_source]
                if self.direct_list_no_source is not None
                else None
            ),
            direct_no_source=self.direct_no_source,
            inwards_single=(
                self.inwards_single.as_write() if isinstance(self.inwards_single, DomainModel) else self.inwards_single
            ),
            inwards_single_property=(
                self.inwards_single_property.as_write()
                if isinstance(self.inwards_single_property, DomainRelation)
                else self.inwards_single_property
            ),
            name=self.name,
        )

    def as_apply(self) -> ConnectionItemEWrite:
        """Convert this read version of connection item e to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ConnectionItemE],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._connection_edge_a import ConnectionEdgeA
        from ._connection_item_d import ConnectionItemD

        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
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

                    if edge_type == dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle") and isinstance(
                        value, ConnectionItemD | str | dm.NodeId
                    ):
                        if instance.inwards_single is None:
                            instance.inwards_single = value
                        elif are_nodes_equal(value, instance.inwards_single):
                            instance.inwards_single = select_best_node(value, instance.inwards_single)
                        else:
                            warnings.warn(
                                f"Expected one edge for 'inwards_single' in {instance.as_id()}."
                                f"Ignoring new edge {value!s} in favor of {instance.inwards_single!s}.",
                                stacklevel=2,
                            )
                    if edge_type == dm.DirectRelationReference("sp_pygen_models", "multiProperty") and isinstance(
                        value, ConnectionEdgeA
                    ):
                        if instance.inwards_single_property is None:
                            instance.inwards_single_property = value
                        elif instance.inwards_single_property == value:
                            # This is the same edge, so we don't need to do anything...
                            ...
                        else:
                            warnings.warn(
                                f"Expected one edge for 'inwards_single_property' in {instance.as_id()}."
                                f"Ignoring new edge {value!s} in favor of {instance.inwards_single_property!s}.",
                                stacklevel=2,
                            )

                        if end_node := nodes_by_id.get(as_pygen_node_id(value.end_node)):
                            value.end_node = end_node  # type: ignore[assignment]

        for node in nodes_by_id.values():
            if (
                isinstance(node, ConnectionItemD)
                and node.direct_single is not None
                and (direct_single := instances.get(as_pygen_node_id(node.direct_single)))
            ):
                if direct_single.direct_reverse_single is None:
                    direct_single.direct_reverse_single = node
                elif are_nodes_equal(node, direct_single.direct_reverse_single):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'direct_reverse_single' in {direct_single.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {direct_single.direct_reverse_single!s}.",
                        stacklevel=2,
                    )
            if isinstance(node, ConnectionItemD) and node.direct_multi is not None:
                for direct_multi in node.direct_multi:
                    if this_instance := instances.get(as_pygen_node_id(direct_multi)):
                        if this_instance.direct_reverse_multi is None:
                            this_instance.direct_reverse_multi = [node]
                        else:
                            this_instance.direct_reverse_multi.append(node)


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

        if self.direct_list_no_source is not None:
            properties["directListNoSource"] = [
                {
                    "space": self.space if isinstance(direct_list_no_source, str) else direct_list_no_source.space,
                    "externalId": (
                        direct_list_no_source
                        if isinstance(direct_list_no_source, str)
                        else direct_list_no_source.external_id
                    ),
                }
                for direct_list_no_source in self.direct_list_no_source or []
            ]

        if self.direct_no_source is not None:
            properties["directNoSource"] = {
                "space": self.space if isinstance(self.direct_no_source, str) else self.direct_no_source.space,
                "externalId": (
                    self.direct_no_source
                    if isinstance(self.direct_no_source, str)
                    else self.direct_no_source.external_id
                ),
            }

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

        if self.inwards_single_property is not None:
            other_resources = self.inwards_single_property._to_instances_write(
                cache,
                self,
                dm.DirectRelationReference("sp_pygen_models", "multiProperty"),
            )
            resources.extend(other_resources)

        if self.inwards_single is not None:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self.inwards_single,
                end_node=self,
                edge_type=dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle"),
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemEApply(ConnectionItemEWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemEApply:
        warnings.warn(
            "ConnectionItemEApply is deprecated and will be removed in v1.0. "
            "Use ConnectionItemEWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemE.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemEList(DomainModelList[ConnectionItemE]):
    """List of connection item es in the read version."""

    _INSTANCE = ConnectionItemE

    def as_write(self) -> ConnectionItemEWriteList:
        """Convert these read versions of connection item e to the writing versions."""
        return ConnectionItemEWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemEWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class ConnectionItemEApplyList(ConnectionItemEWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ConnectionItemDQuery not in created_types:
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
                connection_type="reverse-list",
            )

        if _ConnectionItemDQuery not in created_types:
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
            )

        if _ConnectionItemDQuery not in created_types:
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
            )

        if _ConnectionEdgeAQuery not in created_types:
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

    def list_connection_item_e(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemEList:
        return self._list(limit=limit)


class ConnectionItemEQuery(_ConnectionItemEQuery[ConnectionItemEList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemEList)
