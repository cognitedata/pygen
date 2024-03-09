from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._connection_item_e import ConnectionItemE, ConnectionItemEWrite


__all__ = [
    "ConnectionItemD",
    "ConnectionItemDWrite",
    "ConnectionItemDApply",
    "ConnectionItemDList",
    "ConnectionItemDWriteList",
    "ConnectionItemDApplyList",
    "ConnectionItemDFields",
    "ConnectionItemDTextFields",
]


ConnectionItemDTextFields = Literal["name"]
ConnectionItemDFields = Literal["name"]

_CONNECTIONITEMD_PROPERTIES_BY_FIELD = {
    "name": "name",
}


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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemD")
    direct_multi: Union[ConnectionItemE, str, dm.NodeId, None] = Field(None, repr=False, alias="directMulti")
    direct_single: Union[ConnectionItemE, str, dm.NodeId, None] = Field(None, repr=False, alias="directSingle")
    name: Optional[str] = None
    outwards_single: Union[ConnectionItemE, str, dm.NodeId, None] = Field(None, repr=False, alias="outwardsSingle")

    def as_write(self) -> ConnectionItemDWrite:
        """Convert this read version of connection item d to the writing version."""
        return ConnectionItemDWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            direct_multi=(
                self.direct_multi.as_write() if isinstance(self.direct_multi, DomainModel) else self.direct_multi
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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemD")
    direct_multi: Union[ConnectionItemEWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="directMulti")
    direct_single: Union[ConnectionItemEWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="directSingle")
    name: Optional[str] = None
    outwards_single: Union[ConnectionItemEWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="outwardsSingle")

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

        write_view = (view_by_read_class or {}).get(ConnectionItemD, dm.ViewId("pygen-models", "ConnectionItemD", "1"))

        properties: dict[str, Any] = {}

        if self.direct_multi is not None:
            properties["directMulti"] = {
                "space": self.space if isinstance(self.direct_multi, str) else self.direct_multi.space,
                "externalId": (
                    self.direct_multi if isinstance(self.direct_multi, str) else self.direct_multi.external_id
                ),
            }

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

        if isinstance(self.direct_multi, DomainModelWrite):
            other_resources = self.direct_multi._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.direct_single, DomainModelWrite):
            other_resources = self.direct_single._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("pygen-models", "bidirectionalSingle")
        if self.outwards_single is not None:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=self.outwards_single,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.outwards_single, DomainModelWrite):
            other_resources = self.outwards_single._to_instances_write(cache, view_by_read_class)
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


class ConnectionItemDWriteList(DomainModelWriteList[ConnectionItemDWrite]):
    """List of connection item ds in the writing version."""

    _INSTANCE = ConnectionItemDWrite


class ConnectionItemDApplyList(ConnectionItemDWriteList): ...


def _create_connection_item_d_filter(
    view_id: dm.ViewId,
    direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if direct_multi and isinstance(direct_multi, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directMulti"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": direct_multi},
            )
        )
    if direct_multi and isinstance(direct_multi, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directMulti"), value={"space": direct_multi[0], "externalId": direct_multi[1]}
            )
        )
    if direct_multi and isinstance(direct_multi, list) and isinstance(direct_multi[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directMulti"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in direct_multi],
            )
        )
    if direct_multi and isinstance(direct_multi, list) and isinstance(direct_multi[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directMulti"),
                values=[{"space": item[0], "externalId": item[1]} for item in direct_multi],
            )
        )
    if direct_single and isinstance(direct_single, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directSingle"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": direct_single},
            )
        )
    if direct_single and isinstance(direct_single, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directSingle"),
                value={"space": direct_single[0], "externalId": direct_single[1]},
            )
        )
    if direct_single and isinstance(direct_single, list) and isinstance(direct_single[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directSingle"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in direct_single],
            )
        )
    if direct_single and isinstance(direct_single, list) and isinstance(direct_single[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directSingle"),
                values=[{"space": item[0], "externalId": item[1]} for item in direct_single],
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
