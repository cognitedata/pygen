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
    from ._connection_edge_a import ConnectionEdgeA, ConnectionEdgeAGraphQL, ConnectionEdgeAWrite


__all__ = [
    "ConnectionItemG",
    "ConnectionItemGWrite",
    "ConnectionItemGApply",
    "ConnectionItemGList",
    "ConnectionItemGWriteList",
    "ConnectionItemGApplyList",
    "ConnectionItemGFields",
    "ConnectionItemGTextFields",
]


ConnectionItemGTextFields = Literal["name"]
ConnectionItemGFields = Literal["name"]

_CONNECTIONITEMG_PROPERTIES_BY_FIELD = {
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

    view_id = dm.ViewId("pygen-models", "ConnectionItemG", "1")
    inwards_multi_property: Optional[list[ConnectionEdgeAGraphQL]] = Field(
        default=None, repr=False, alias="inwardsMultiProperty"
    )
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

    @validator("inwards_multi_property", pre=True)
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemG:
        """Convert this GraphQL format of connection item g to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemG(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            inwards_multi_property=[
                inwards_multi_property.as_read() for inwards_multi_property in self.inwards_multi_property or []
            ],
            name=self.name,
        )

    def as_write(self) -> ConnectionItemGWrite:
        """Convert this GraphQL format of connection item g to the writing format."""
        return ConnectionItemGWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            inwards_multi_property=[
                inwards_multi_property.as_write() for inwards_multi_property in self.inwards_multi_property or []
            ],
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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemG")
    inwards_multi_property: Optional[list[ConnectionEdgeA]] = Field(
        default=None, repr=False, alias="inwardsMultiProperty"
    )
    name: Optional[str] = None

    def as_write(self) -> ConnectionItemGWrite:
        """Convert this read version of connection item g to the writing version."""
        return ConnectionItemGWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            inwards_multi_property=[
                inwards_multi_property.as_write() for inwards_multi_property in self.inwards_multi_property or []
            ],
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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemG")
    inwards_multi_property: Optional[list[ConnectionEdgeAWrite]] = Field(
        default=None, repr=False, alias="inwardsMultiProperty"
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

        write_view = (view_by_read_class or {}).get(ConnectionItemG, dm.ViewId("pygen-models", "ConnectionItemG", "1"))

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
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        for inwards_multi_property in self.inwards_multi_property or []:
            if isinstance(inwards_multi_property, DomainRelationWrite):
                other_resources = inwards_multi_property._to_instances_write(
                    cache, self, dm.DirectRelationReference("pygen-models", "multiProperty"), view_by_read_class
                )
                resources.extend(other_resources)

        return resources


class ConnectionItemGApply(ConnectionItemGWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemGApply:
        warnings.warn(
            "ConnectionItemGApply is deprecated and will be removed in v1.0. Use ConnectionItemGWrite instead."
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


class ConnectionItemGWriteList(DomainModelWriteList[ConnectionItemGWrite]):
    """List of connection item gs in the writing version."""

    _INSTANCE = ConnectionItemGWrite


class ConnectionItemGApplyList(ConnectionItemGWriteList): ...


def _create_connection_item_g_filter(
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
