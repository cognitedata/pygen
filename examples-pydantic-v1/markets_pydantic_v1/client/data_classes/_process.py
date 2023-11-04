from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._bid import BidApply

__all__ = ["Process", "ProcessApply", "ProcessList", "ProcessApplyList", "ProcessFields", "ProcessTextFields"]


ProcessTextFields = Literal["name"]
ProcessFields = Literal["name"]

_PROCESS_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class Process(DomainModel):
    """This represent a read version of proces.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the proces.
        bid: The bid field.
        name: The name field.
        created_time: The created time of the proces node.
        last_updated_time: The last updated time of the proces node.
        deleted_time: If present, the deleted time of the proces node.
        version: The version of the proces node.
    """

    space: str = "market"
    bid: Optional[str] = None
    name: Optional[str] = None

    def as_apply(self) -> ProcessApply:
        """Convert this read version of proces to a write version."""
        return ProcessApply(
            space=self.space,
            external_id=self.external_id,
            bid=self.bid,
            name=self.name,
        )


class ProcessApply(DomainModelApply):
    """This represent a write version of proces.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the proces.
        bid: The bid field.
        name: The name field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    bid: Union[BidApply, str, None] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.bid is not None:
            properties["bid"] = {
                "space": "market",
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "Process", "98a2becd0f63ee"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ProcessList(TypeList[Process]):
    """List of process in read version."""

    _NODE = Process

    def as_apply(self) -> ProcessApplyList:
        """Convert this read version of proces to a write version."""
        return ProcessApplyList([node.as_apply() for node in self.data])


class ProcessApplyList(TypeApplyList[ProcessApply]):
    """List of process in write version."""

    _NODE = ProcessApply
