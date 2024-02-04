from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._connection_item_a import ConnectionItemA, ConnectionItemAApply
    from ._connection_item_b import ConnectionItemB, ConnectionItemBApply
    from ._connection_item_c import ConnectionItemC, ConnectionItemCApply


__all__ = [
    "ConnectionItemA",
    "ConnectionItemAApply",
    "ConnectionItemAList",
    "ConnectionItemAApplyList",
    "ConnectionItemAFields",
    "ConnectionItemATextFields",
]


ConnectionItemATextFields = Literal["name"]
ConnectionItemAFields = Literal["name"]

_CONNECTIONITEMA_PROPERTIES_BY_FIELD = {
    "name": "name",
}


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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemA")
    name: Optional[str] = None
    other_direct: Union[ConnectionItemC, str, dm.NodeId, None] = Field(None, repr=False, alias="otherDirect")
    outwards: Union[list[ConnectionItemB], list[str], None] = Field(default=None, repr=False)
    self_direct: Union[ConnectionItemA, str, dm.NodeId, None] = Field(None, repr=False, alias="selfDirect")

    def as_apply(self) -> ConnectionItemAApply:
        """Convert this read version of connection item a to the writing version."""
        return ConnectionItemAApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            other_direct=(
                self.other_direct.as_apply() if isinstance(self.other_direct, DomainModel) else self.other_direct
            ),
            outwards=[
                outward.as_apply() if isinstance(outward, DomainModel) else outward for outward in self.outwards or []
            ],
            self_direct=self.self_direct.as_apply() if isinstance(self.self_direct, DomainModel) else self.self_direct,
        )


class ConnectionItemAApply(DomainModelApply):
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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemA")
    name: Optional[str] = None
    other_direct: Union[ConnectionItemCApply, str, dm.NodeId, None] = Field(None, repr=False, alias="otherDirect")
    outwards: Union[list[ConnectionItemBApply], list[str], None] = Field(default=None, repr=False)
    self_direct: Union[ConnectionItemAApply, str, dm.NodeId, None] = Field(None, repr=False, alias="selfDirect")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(ConnectionItemA, dm.ViewId("pygen-models", "ConnectionItemA", "1"))

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
                existing_version=self.data_record.existing_version,
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

        edge_type = dm.DirectRelationReference("pygen-models", "bidirectional")
        for outward in self.outwards or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=outward, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.other_direct, DomainModelApply):
            other_resources = self.other_direct._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.self_direct, DomainModelApply):
            other_resources = self.self_direct._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class ConnectionItemAList(DomainModelList[ConnectionItemA]):
    """List of connection item as in the read version."""

    _INSTANCE = ConnectionItemA

    def as_apply(self) -> ConnectionItemAApplyList:
        """Convert these read versions of connection item a to the writing versions."""
        return ConnectionItemAApplyList([node.as_apply() for node in self.data])


class ConnectionItemAApplyList(DomainModelApplyList[ConnectionItemAApply]):
    """List of connection item as in the writing version."""

    _INSTANCE = ConnectionItemAApply


def _create_connection_item_a_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    other_direct: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    self_direct: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if other_direct and isinstance(other_direct, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("otherDirect"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": other_direct},
            )
        )
    if other_direct and isinstance(other_direct, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("otherDirect"), value={"space": other_direct[0], "externalId": other_direct[1]}
            )
        )
    if other_direct and isinstance(other_direct, list) and isinstance(other_direct[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("otherDirect"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in other_direct],
            )
        )
    if other_direct and isinstance(other_direct, list) and isinstance(other_direct[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("otherDirect"),
                values=[{"space": item[0], "externalId": item[1]} for item in other_direct],
            )
        )
    if self_direct and isinstance(self_direct, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("selfDirect"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": self_direct},
            )
        )
    if self_direct and isinstance(self_direct, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("selfDirect"), value={"space": self_direct[0], "externalId": self_direct[1]}
            )
        )
    if self_direct and isinstance(self_direct, list) and isinstance(self_direct[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("selfDirect"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in self_direct],
            )
        )
    if self_direct and isinstance(self_direct, list) and isinstance(self_direct[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("selfDirect"),
                values=[{"space": item[0], "externalId": item[1]} for item in self_direct],
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
