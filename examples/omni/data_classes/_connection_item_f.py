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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    is_tuple_id,
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
    "ConnectionItemF",
    "ConnectionItemFWrite",
    "ConnectionItemFApply",
    "ConnectionItemFList",
    "ConnectionItemFWriteList",
    "ConnectionItemFApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ConnectionItemF:
        """Convert this GraphQL format of connection item f to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemF(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            direct_list=(
                [direct_list.as_read() for direct_list in self.direct_list] if self.direct_list is not None else None
            ),
            name=self.name,
            outwards_multi=(
                [outwards_multi.as_read() for outwards_multi in self.outwards_multi]
                if self.outwards_multi is not None
                else None
            ),
            outwards_single=(
                self.outwards_single.as_read()
                if isinstance(self.outwards_single, GraphQLCore)
                else self.outwards_single
            ),
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemFWrite:
        """Convert this GraphQL format of connection item f to the writing format."""
        return ConnectionItemFWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            direct_list=(
                [direct_list.as_write() for direct_list in self.direct_list] if self.direct_list is not None else None
            ),
            name=self.name,
            outwards_multi=(
                [outwards_multi.as_write() for outwards_multi in self.outwards_multi]
                if self.outwards_multi is not None
                else None
            ),
            outwards_single=(
                self.outwards_single.as_write()
                if isinstance(self.outwards_single, GraphQLCore)
                else self.outwards_single
            ),
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemFWrite:
        """Convert this read version of connection item f to the writing version."""
        return ConnectionItemFWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            direct_list=(
                [
                    direct_list.as_write() if isinstance(direct_list, DomainModel) else direct_list
                    for direct_list in self.direct_list
                ]
                if self.direct_list is not None
                else None
            ),
            name=self.name,
            outwards_multi=(
                [outwards_multi.as_write() for outwards_multi in self.outwards_multi]
                if self.outwards_multi is not None
                else None
            ),
            outwards_single=(
                self.outwards_single.as_write()
                if isinstance(self.outwards_single, DomainRelation)
                else self.outwards_single
            ),
        )

    def as_apply(self) -> ConnectionItemFWrite:
        """Convert this read version of connection item f to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ConnectionItemF],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._connection_edge_a import ConnectionEdgeA
        from ._connection_item_d import ConnectionItemD

        for instance in instances.values():
            if instance.direct_list:
                new_direct_list: list[ConnectionItemD | str | dm.NodeId] = []
                for direct_list in instance.direct_list:
                    if isinstance(direct_list, ConnectionItemD):
                        new_direct_list.append(direct_list)
                    elif (other := nodes_by_id.get(direct_list)) and isinstance(other, ConnectionItemD):
                        new_direct_list.append(other)
                    else:
                        new_direct_list.append(direct_list)
                instance.direct_list = new_direct_list
            if edges := edges_by_source_node.get(instance.as_id()):
                outwards_multi: list[ConnectionEdgeA] = []
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
                        outwards_multi.append(value)
                        if end_node := nodes_by_id.get(as_pygen_node_id(value.end_node)):
                            value.end_node = end_node  # type: ignore[assignment]
                    if edge_type == dm.DirectRelationReference("sp_pygen_models", "singleProperty") and isinstance(
                        value, ConnectionEdgeA
                    ):
                        if instance.outwards_single is None:
                            instance.outwards_single = value
                        elif instance.outwards_single == value:
                            # This is the same edge, so we don't need to do anything...
                            ...
                        else:
                            warnings.warn(
                                f"Expected one edge for 'outwards_single' in {instance.as_id()}."
                                f"Ignoring new edge {value!s} in favor of {instance.outwards_single!s}.",
                                stacklevel=2,
                            )

                        if end_node := nodes_by_id.get(as_pygen_node_id(value.end_node)):
                            value.end_node = end_node  # type: ignore[assignment]

                instance.outwards_multi = outwards_multi


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

        if self.direct_list is not None:
            properties["directList"] = [
                {
                    "space": self.space if isinstance(direct_list, str) else direct_list.space,
                    "externalId": direct_list if isinstance(direct_list, str) else direct_list.external_id,
                }
                for direct_list in self.direct_list or []
            ]

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

        for outwards_multi in self.outwards_multi or []:
            if isinstance(outwards_multi, DomainRelationWrite):
                other_resources = outwards_multi._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("sp_pygen_models", "multiProperty"),
                )
                resources.extend(other_resources)

        if self.outwards_single is not None:
            other_resources = self.outwards_single._to_instances_write(
                cache,
                self,
                dm.DirectRelationReference("sp_pygen_models", "singleProperty"),
            )
            resources.extend(other_resources)

        for direct_list in self.direct_list or []:
            if isinstance(direct_list, DomainModelWrite):
                other_resources = direct_list._to_instances_write(cache)
                resources.extend(other_resources)

        return resources


class ConnectionItemFApply(ConnectionItemFWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemFApply:
        warnings.warn(
            "ConnectionItemFApply is deprecated and will be removed in v1.0. Use ConnectionItemFWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemF.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemFList(DomainModelList[ConnectionItemF]):
    """List of connection item fs in the read version."""

    _INSTANCE = ConnectionItemF

    def as_write(self) -> ConnectionItemFWriteList:
        """Convert these read versions of connection item f to the writing versions."""
        return ConnectionItemFWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemFWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class ConnectionItemFApplyList(ConnectionItemFWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ConnectionItemDQuery not in created_types:
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
            )

        if _ConnectionEdgeAQuery not in created_types:
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
            )

        if _ConnectionEdgeAQuery not in created_types:
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
