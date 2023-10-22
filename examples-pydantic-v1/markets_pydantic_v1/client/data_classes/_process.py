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
    "bid": "bid",
    "name": "name",
}


class Process(DomainModel):
    space: str = "market"
    bid: Optional[str] = None
    name: Optional[str] = None

    def as_apply(self) -> ProcessApply:
        return ProcessApply(
            external_id=self.external_id,
            bid=self.bid,
            name=self.name,
        )


class ProcessApply(DomainModelApply):
    space: str = "market"
    bid: Union[BidApply, str, None] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
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
                source=dm.ContainerId("market", "Process"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ProcessList(TypeList[Process]):
    _NODE = Process

    def as_apply(self) -> ProcessApplyList:
        return ProcessApplyList([node.as_apply() for node in self.data])


class ProcessApplyList(TypeApplyList[ProcessApply]):
    _NODE = ProcessApply
