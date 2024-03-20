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
        inwards_single: The inwards single field.
        name: The name field.
    """

    view_id = dm.ViewId("pygen-models", "ConnectionItemE", "1")
    direct_no_source: Optional[str] = Field(None, alias="directNoSource")
    inwards_single: Optional[list[ConnectionItemDGraphQL]] = Field(None, repr=False, alias="inwardsSingle")
    name: Optional[str] = None

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

    @validator("direct_no_source", "inwards_single", pre=True)
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

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
            direct_no_source=self.direct_no_source,
            inwards_single=(
                self.inwards_single.as_read() if isinstance(self.inwards_single, GraphQLCore) else self.inwards_single
            ),
            name=self.name,
        )

    def as_write(self) -> ConnectionItemEWrite:
        """Convert this GraphQL format of connection item e to the writing format."""
        return ConnectionItemEWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            direct_no_source=self.direct_no_source,
            inwards_single=(
                self.inwards_single.as_write() if isinstance(self.inwards_single, DomainModel) else self.inwards_single
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
        direct_no_source: The direct no source field.
        inwards_single: The inwards single field.
        name: The name field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemE")
    direct_no_source: Union[str, dm.NodeId, None] = Field(None, alias="directNoSource")
    inwards_single: Union[list[ConnectionItemD], list[str], list[dm.NodeId], None] = Field(
        None, repr=False, alias="inwardsSingle"
    )
    name: Optional[str] = None

    def as_write(self) -> ConnectionItemEWrite:
        """Convert this read version of connection item e to the writing version."""
        return ConnectionItemEWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            direct_no_source=self.direct_no_source,
            inwards_single=(
                self.inwards_single.as_write() if isinstance(self.inwards_single, DomainModel) else self.inwards_single
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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemE")
    direct_no_source: Union[str, dm.NodeId, None] = Field(None, alias="directNoSource")
    inwards_single: Union[list[ConnectionItemDWrite], list[str], list[dm.NodeId], None] = Field(
        None, repr=False, alias="inwardsSingle"
    )
    name: Optional[str] = None

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

        write_view = (view_by_read_class or {}).get(ConnectionItemE, dm.ViewId("pygen-models", "ConnectionItemE", "1"))

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
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        for inwards_single in self.inwards_single or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=inwards_single,
                end_node=self,
                edge_type=dm.DirectRelationReference("pygen-models", "bidirectionalSingle"),
                view_by_read_class=view_by_read_class,
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
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
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
