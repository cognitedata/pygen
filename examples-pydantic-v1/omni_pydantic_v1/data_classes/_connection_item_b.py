from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
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


__all__ = [
    "ConnectionItemB",
    "ConnectionItemBApply",
    "ConnectionItemBList",
    "ConnectionItemBApplyList",
    "ConnectionItemBFields",
    "ConnectionItemBTextFields",
]


ConnectionItemBTextFields = Literal["name"]
ConnectionItemBFields = Literal["name"]

_CONNECTIONITEMB_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class ConnectionItemB(DomainModel):
    """This represents the reading version of connection item b.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item b.
        inwards: The inward field.
        name: The name field.
        self_edge: The self edge field.
        created_time: The created time of the connection item b node.
        last_updated_time: The last updated time of the connection item b node.
        deleted_time: If present, the deleted time of the connection item b node.
        version: The version of the connection item b node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemB")
    inwards: Union[list[ConnectionItemA], list[str], None] = Field(default=None, repr=False)
    name: Optional[str] = None
    self_edge: Union[list[ConnectionItemB], list[str], None] = Field(default=None, repr=False, alias="selfEdge")

    def as_apply(self) -> ConnectionItemBApply:
        """Convert this read version of connection item b to the writing version."""
        return ConnectionItemBApply(
            space=self.space,
            external_id=self.external_id,
            inwards=[inward.as_apply() if isinstance(inward, DomainModel) else inward for inward in self.inwards or []],
            name=self.name,
            self_edge=[
                self_edge.as_apply() if isinstance(self_edge, DomainModel) else self_edge
                for self_edge in self.self_edge or []
            ],
        )


class ConnectionItemBApply(DomainModelApply):
    """This represents the writing version of connection item b.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item b.
        inwards: The inward field.
        name: The name field.
        self_edge: The self edge field.
        existing_version: Fail the ingestion request if the connection item b version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemB")
    inwards: Union[list[ConnectionItemAApply], list[str], None] = Field(default=None, repr=False)
    name: Optional[str] = None
    self_edge: Union[list[ConnectionItemBApply], list[str], None] = Field(default=None, repr=False, alias="selfEdge")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(ConnectionItemB, dm.ViewId("pygen-models", "ConnectionItemB", "1"))

        properties = {}

        if self.name is not None:
            properties["name"] = self.name

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
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
        for inward in self.inwards or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=inward, end_node=self, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("pygen-models", "reflexive")
        for self_edge in self.self_edge or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=self_edge, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemBList(DomainModelList[ConnectionItemB]):
    """List of connection item bs in the read version."""

    _INSTANCE = ConnectionItemB

    def as_apply(self) -> ConnectionItemBApplyList:
        """Convert these read versions of connection item b to the writing versions."""
        return ConnectionItemBApplyList([node.as_apply() for node in self.data])


class ConnectionItemBApplyList(DomainModelApplyList[ConnectionItemBApply]):
    """List of connection item bs in the writing version."""

    _INSTANCE = ConnectionItemBApply


def _create_connection_item_b_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None