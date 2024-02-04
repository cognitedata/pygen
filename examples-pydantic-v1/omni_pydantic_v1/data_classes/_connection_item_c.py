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


__all__ = ["ConnectionItemC", "ConnectionItemCApply", "ConnectionItemCList", "ConnectionItemCApplyList"]


class ConnectionItemC(DomainModel):
    """This represents the reading version of connection item c.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c.
        data_record: The data record of the connection item c node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemC")
    connection_item_a: Union[list[ConnectionItemA], list[str], None] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Union[list[ConnectionItemB], list[str], None] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    def as_apply(self) -> ConnectionItemCApply:
        """Convert this read version of connection item c to the writing version."""
        return ConnectionItemCApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            connection_item_a=[
                connection_item_a.as_apply() if isinstance(connection_item_a, DomainModel) else connection_item_a
                for connection_item_a in self.connection_item_a or []
            ],
            connection_item_b=[
                connection_item_b.as_apply() if isinstance(connection_item_b, DomainModel) else connection_item_b
                for connection_item_b in self.connection_item_b or []
            ],
        )


class ConnectionItemCApply(DomainModelApply):
    """This represents the writing version of connection item c.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item c.
        data_record: The data record of the connection item c node.
        connection_item_a: The connection item a field.
        connection_item_b: The connection item b field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemC")
    connection_item_a: Union[list[ConnectionItemAApply], list[str], None] = Field(
        default=None, repr=False, alias="connectionItemA"
    )
    connection_item_b: Union[list[ConnectionItemBApply], list[str], None] = Field(
        default=None, repr=False, alias="connectionItemB"
    )

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources
        cache.add(self.as_tuple_id())

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.data_record.existing_version,
            type=self.node_type,
            sources=None,
        )
        resources.nodes.append(this_node)

        edge_type = dm.DirectRelationReference("pygen-models", "unidirectional")
        for connection_item_a in self.connection_item_a or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=connection_item_a,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("pygen-models", "unidirectional")
        for connection_item_b in self.connection_item_b or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=connection_item_b,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
            )
            resources.extend(other_resources)

        return resources


class ConnectionItemCList(DomainModelList[ConnectionItemC]):
    """List of connection item cs in the read version."""

    _INSTANCE = ConnectionItemC

    def as_apply(self) -> ConnectionItemCApplyList:
        """Convert these read versions of connection item c to the writing versions."""
        return ConnectionItemCApplyList([node.as_apply() for node in self.data])


class ConnectionItemCApplyList(DomainModelApplyList[ConnectionItemCApply]):
    """List of connection item cs in the writing version."""

    _INSTANCE = ConnectionItemCApply


def _create_connection_item_c_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
