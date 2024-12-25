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
    "ConnectionItemDApply",
    "ConnectionItemDList",
    "ConnectionItemDWriteList",
    "ConnectionItemDApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemD:
        """Convert this GraphQL format of connection item d to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemD(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            direct_multi=(
                [direct_multi.as_read() for direct_multi in self.direct_multi]
                if self.direct_multi is not None
                else None
            ),
            direct_single=(
                self.direct_single.as_read() if isinstance(self.direct_single, GraphQLCore) else self.direct_single
            ),
            name=self.name,
            outwards_single=(
                self.outwards_single.as_read()
                if isinstance(self.outwards_single, GraphQLCore)
                else self.outwards_single
            ),
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemDWrite:
        """Convert this GraphQL format of connection item d to the writing format."""
        return ConnectionItemDWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            direct_multi=(
                [direct_multi.as_write() for direct_multi in self.direct_multi]
                if self.direct_multi is not None
                else None
            ),
            direct_single=(
                self.direct_single.as_write() if isinstance(self.direct_single, GraphQLCore) else self.direct_single
            ),
            name=self.name,
            outwards_single=(
                self.outwards_single.as_write()
                if isinstance(self.outwards_single, GraphQLCore)
                else self.outwards_single
            ),
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemDWrite:
        """Convert this read version of connection item d to the writing version."""
        return ConnectionItemDWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            direct_multi=(
                [
                    direct_multi.as_write() if isinstance(direct_multi, DomainModel) else direct_multi
                    for direct_multi in self.direct_multi
                ]
                if self.direct_multi is not None
                else None
            ),
            direct_single=(
                self.direct_single.as_write() if isinstance(self.direct_single, DomainModel) else self.direct_single
            ),
            name=self.name,
            outwards_single=(
                self.outwards_single.as_write()
                if isinstance(self.outwards_single, DomainModel)
                else self.outwards_single
            ),
        )

    def as_apply(self) -> ConnectionItemDWrite:
        """Convert this read version of connection item d to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ConnectionItemD],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._connection_item_e import ConnectionItemE

        for instance in instances.values():
            if (
                isinstance(instance.direct_single, dm.NodeId | str)
                and (direct_single := nodes_by_id.get(instance.direct_single))
                and isinstance(direct_single, ConnectionItemE)
            ):
                instance.direct_single = direct_single
            if instance.direct_multi:
                new_direct_multi: list[ConnectionItemE | str | dm.NodeId] = []
                for direct_multi in instance.direct_multi:
                    if isinstance(direct_multi, ConnectionItemE):
                        new_direct_multi.append(direct_multi)
                    elif (other := nodes_by_id.get(direct_multi)) and isinstance(other, ConnectionItemE):
                        new_direct_multi.append(other)
                    else:
                        new_direct_multi.append(direct_multi)
                instance.direct_multi = new_direct_multi
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
                        value, (ConnectionItemE, str, dm.NodeId)
                    ):
                        if instance.outwards_single is None:
                            instance.outwards_single = value
                        elif are_nodes_equal(value, instance.outwards_single):
                            instance.outwards_single = select_best_node(value, instance.outwards_single)
                        else:
                            warnings.warn(
                                f"Expected one edge for 'outwards_single' in {instance.as_id()}."
                                f"Ignoring new edge {value!s} in favor of {instance.outwards_single!s}.",
                                stacklevel=2,
                            )


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

        if self.direct_multi is not None:
            properties["directMulti"] = [
                {
                    "space": self.space if isinstance(direct_multi, str) else direct_multi.space,
                    "externalId": direct_multi if isinstance(direct_multi, str) else direct_multi.external_id,
                }
                for direct_multi in self.direct_multi or []
            ]

        if self.direct_single is not None:
            properties["directSingle"] = {
                "space": self.space if isinstance(self.direct_single, str) else self.direct_single.space,
                "externalId": (
                    self.direct_single if isinstance(self.direct_single, str) else self.direct_single.external_id
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

        if isinstance(self.direct_single, DomainModelWrite):
            other_resources = self.direct_single._to_instances_write(cache)
            resources.extend(other_resources)

        for direct_multi in self.direct_multi or []:
            if isinstance(direct_multi, DomainModelWrite):
                other_resources = direct_multi._to_instances_write(cache)
                resources.extend(other_resources)

        if self.outwards_single is not None:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=self.outwards_single,
                edge_type=dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle"),
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemDApply(ConnectionItemDWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemDApply:
        warnings.warn(
            "ConnectionItemDApply is deprecated and will be removed in v1.0. Use ConnectionItemDWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemD.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemDList(DomainModelList[ConnectionItemD]):
    """List of connection item ds in the read version."""

    _INSTANCE = ConnectionItemD

    def as_write(self) -> ConnectionItemDWriteList:
        """Convert these read versions of connection item d to the writing versions."""
        return ConnectionItemDWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemDWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class ConnectionItemDApplyList(ConnectionItemDWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ConnectionItemEQuery not in created_types:
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
            )

        if _ConnectionItemEQuery not in created_types:
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
            )

        if _ConnectionItemEQuery not in created_types:
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

    def list_connection_item_d(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemDList:
        return self._list(limit=limit)


class ConnectionItemDQuery(_ConnectionItemDQuery[ConnectionItemDList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemDList)
