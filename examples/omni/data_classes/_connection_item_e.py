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
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
)

if TYPE_CHECKING:
    from ._connection_item_d import ConnectionItemD, ConnectionItemDGraphQL, ConnectionItemDWrite


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


ConnectionItemETextFields = Literal["name"]
ConnectionItemEFields = Literal["name"]

_CONNECTIONITEME_PROPERTIES_BY_FIELD = {
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
        direct_no_source: The direct no source field.
        direct_reverse_multi: The direct reverse multi field.
        direct_reverse_single: The direct reverse single field.
        inwards_single: The inwards single field.
        name: The name field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemE", "1")
    direct_no_source: Optional[str] = Field(default=None, alias="directNoSource")
    direct_reverse_multi: Optional[list[ConnectionItemDGraphQL]] = Field(
        default=None, repr=False, alias="directReverseMulti"
    )
    direct_reverse_single: Optional[ConnectionItemDGraphQL] = Field(
        default=None, repr=False, alias="directReverseSingle"
    )
    inwards_single: Optional[list[ConnectionItemDGraphQL]] = Field(default=None, repr=False, alias="inwardsSingle")
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
        "direct_no_source", "direct_reverse_multi", "direct_reverse_single", "inwards_single", mode="before"
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
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            direct_no_source=self.direct_no_source,
            direct_reverse_multi=[
                direct_reverse_multi.as_read() for direct_reverse_multi in self.direct_reverse_multi or []
            ],
            direct_reverse_single=(
                self.direct_reverse_single.as_read()
                if isinstance(self.direct_reverse_single, GraphQLCore)
                else self.direct_reverse_single
            ),
            inwards_single=[inwards_single.as_read() for inwards_single in self.inwards_single or []],
            name=self.name,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemEWrite:
        """Convert this GraphQL format of connection item e to the writing format."""
        return ConnectionItemEWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            direct_no_source=self.direct_no_source,
            inwards_single=[inwards_single.as_write() for inwards_single in self.inwards_single or []],
            name=self.name,
        )


class ConnectionItemE(DomainModel):
    """This represents the reading version of connection item e.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item e.
        data_record: The data record of the connection item e node.
        direct_no_source: The direct no source field.
        direct_reverse_multi: The direct reverse multi field.
        direct_reverse_single: The direct reverse single field.
        inwards_single: The inwards single field.
        name: The name field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemE", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemE")
    direct_no_source: Union[str, dm.NodeId, None] = Field(default=None, alias="directNoSource")
    direct_reverse_multi: Optional[list[ConnectionItemD]] = Field(default=None, repr=False, alias="directReverseMulti")
    direct_reverse_single: Optional[ConnectionItemD] = Field(default=None, repr=False, alias="directReverseSingle")
    inwards_single: Optional[list[Union[ConnectionItemD, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="inwardsSingle"
    )
    name: Optional[str] = None

    def as_write(self) -> ConnectionItemEWrite:
        """Convert this read version of connection item e to the writing version."""
        return ConnectionItemEWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            direct_no_source=self.direct_no_source,
            inwards_single=[
                inwards_single.as_write() if isinstance(inwards_single, DomainModel) else inwards_single
                for inwards_single in self.inwards_single or []
            ],
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
        from ._connection_item_d import ConnectionItemD

        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                inwards_single: list[ConnectionItemD | str | dm.NodeId] = []
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

                    if edge_type == dm.DirectRelationReference("pygen-models", "bidirectionalSingle") and isinstance(
                        value, (ConnectionItemD, str, dm.NodeId)
                    ):
                        inwards_single.append(value)

                instance.inwards_single = inwards_single or None

        for node in nodes_by_id.values():
            if (
                isinstance(node, ConnectionItemD)
                and node.direct_single is not None
                and (direct_single := instances.get(as_pygen_node_id(node.direct_single)))
            ):
                node.direct_single = direct_single
                if direct_single.direct_reverse_single is None:
                    direct_single.direct_reverse_single = node
                elif are_nodes_equal(direct_single.direct_reverse_single, node):
                    # This is the same node, so we don't need to do anything...
                    ...
                else:
                    warnings.warn(
                        f"Expected one direct relation for 'direct_reverse_single' in {direct_single.as_id()}."
                        f"Ignoring new relation {node!s} in favor of {direct_single.direct_reverse_single!s}."
                    )
            if (
                isinstance(node, ConnectionItemD)
                and node.direct_multi is not None
                and (direct_multi := instances.get(as_pygen_node_id(node.direct_multi)))
            ):
                node.direct_multi = direct_multi
                if direct_multi.direct_reverse_multi is None:
                    direct_multi.direct_reverse_multi = []
                direct_multi.direct_reverse_multi.append(node)


class ConnectionItemEWrite(DomainModelWrite):
    """This represents the writing version of connection item e.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item e.
        data_record: The data record of the connection item e node.
        direct_no_source: The direct no source field.
        inwards_single: The inwards single field.
        name: The name field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemE", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemE")
    direct_no_source: Union[str, dm.NodeId, None] = Field(default=None, alias="directNoSource")
    inwards_single: Optional[list[Union[ConnectionItemDWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="inwardsSingle"
    )
    name: Optional[str] = None

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
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("pygen-models", "bidirectionalSingle")
        for inwards_single in self.inwards_single or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=inwards_single,
                end_node=self,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemEApply(ConnectionItemEWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemEApply:
        warnings.warn(
            "ConnectionItemEApply is deprecated and will be removed in v1.0. Use ConnectionItemEWrite instead."
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


class ConnectionItemEWriteList(DomainModelWriteList[ConnectionItemEWrite]):
    """List of connection item es in the writing version."""

    _INSTANCE = ConnectionItemEWrite


class ConnectionItemEApplyList(ConnectionItemEWriteList): ...


def _create_connection_item_e_filter(
    view_id: dm.ViewId,
    direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if direct_no_source and isinstance(direct_no_source, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directNoSource"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": direct_no_source},
            )
        )
    if direct_no_source and isinstance(direct_no_source, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directNoSource"),
                value={"space": direct_no_source[0], "externalId": direct_no_source[1]},
            )
        )
    if direct_no_source and isinstance(direct_no_source, list) and isinstance(direct_no_source[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directNoSource"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in direct_no_source],
            )
        )
    if direct_no_source and isinstance(direct_no_source, list) and isinstance(direct_no_source[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directNoSource"),
                values=[{"space": item[0], "externalId": item[1]} for item in direct_no_source],
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
