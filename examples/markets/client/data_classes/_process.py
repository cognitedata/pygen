from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from markets.client.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from markets.client.data_classes._bids import BidApply

__all__ = ["Proces", "ProcesApply", "ProcesList"]


class Proces(DomainModel):
    space: ClassVar[str] = "market"
    bid: Optional[str] = None
    name: Optional[str] = None


class ProcesApply(DomainModelApply):
    space: ClassVar[str] = "market"
    bid: Optional[Union["BidApply", str]] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("market", "Process"),
            properties={
                "bid": {
                    "space": "market",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
                "name": self.name,
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(nodes, edges)


class ProcesList(TypeList[Proces]):
    _NODE = Proces
