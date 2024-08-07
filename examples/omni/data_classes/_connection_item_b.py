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


ConnectionItemBTextFields = Literal["name"]
ConnectionItemBFields = Literal["name"]

_CONNECTIONITEMB_PROPERTIES_BY_FIELD = {
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemB", "1")
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
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            inwards=[inward.as_read() for inward in self.inwards or []],
            name=self.name,
            self_edge=[self_edge.as_read() for self_edge in self.self_edge or []],
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ConnectionItemBWrite:
        """Convert this GraphQL format of connection item b to the writing format."""
        return ConnectionItemBWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            inwards=[inward.as_write() for inward in self.inwards or []],
            name=self.name,
            self_edge=[self_edge.as_write() for self_edge in self.self_edge or []],
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemB", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemB")
    inwards: Optional[list[Union[ConnectionItemA, str, dm.NodeId]]] = Field(default=None, repr=False)
    name: Optional[str] = None
    self_edge: Optional[list[Union[ConnectionItemB, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="selfEdge"
    )

    def as_write(self) -> ConnectionItemBWrite:
        """Convert this read version of connection item b to the writing version."""
        return ConnectionItemBWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            inwards=[inward.as_write() if isinstance(inward, DomainModel) else inward for inward in self.inwards or []],
            name=self.name,
            self_edge=[
                self_edge.as_write() if isinstance(self_edge, DomainModel) else self_edge
                for self_edge in self.self_edge or []
            ],
        )

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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemB", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemB")
    inwards: Optional[list[Union[ConnectionItemAWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    name: Optional[str] = None
    self_edge: Optional[list[Union[ConnectionItemBWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="selfEdge"
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

        properties: dict[str, Any] = {}

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

        edge_type = dm.DirectRelationReference("pygen-models", "bidirectional")
        for inward in self.inwards or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=inward,
                end_node=self,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("pygen-models", "reflexive")
        for self_edge in self.self_edge or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=self_edge,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemBApply(ConnectionItemBWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemBApply:
        warnings.warn(
            "ConnectionItemBApply is deprecated and will be removed in v1.0. Use ConnectionItemBWrite instead."
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


class ConnectionItemBWriteList(DomainModelWriteList[ConnectionItemBWrite]):
    """List of connection item bs in the writing version."""

    _INSTANCE = ConnectionItemBWrite


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
