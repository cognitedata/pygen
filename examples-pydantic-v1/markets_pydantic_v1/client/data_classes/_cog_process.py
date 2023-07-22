from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from markets_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList
from pydantic import Field

if TYPE_CHECKING:
    from markets_pydantic_v1.client.data_classes._bids import BidApply
    from markets_pydantic_v1.client.data_classes._date_transformation_pairs import DateTransformationPairApply
    from markets_pydantic_v1.client.data_classes._value_transformations import ValueTransformationApply

__all__ = ["CogProces", "CogProcesApply", "CogProcesList"]


class CogProces(DomainModel):
    space: ClassVar[str] = "market"
    bid: Optional[str] = None
    date_transformations: Optional[str] = Field(None, alias="dateTransformations")
    name: Optional[str] = None
    transformation: Optional[str] = None


class CogProcesApply(DomainModelApply):
    space: ClassVar[str] = "market"
    bid: Optional[Union["BidApply", str]] = Field(None, repr=False)
    date_transformations: Optional[Union["DateTransformationPairApply", str]] = Field(None, repr=False)
    name: Optional[str] = None
    transformation: Optional[Union["ValueTransformationApply", str]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

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

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("market", "CogProcess"),
            properties={
                "dateTransformations": {
                    "space": "market",
                    "externalId": self.date_transformations
                    if isinstance(self.date_transformations, str)
                    else self.date_transformations.external_id,
                },
                "transformation": {
                    "space": "market",
                    "externalId": self.transformation
                    if isinstance(self.transformation, str)
                    else self.transformation.external_id,
                },
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

        if isinstance(self.date_transformations, DomainModelApply):
            instances = self.date_transformations._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.transformation, DomainModelApply):
            instances = self.transformation._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)


class CogProcesList(TypeList[CogProces]):
    _NODE = CogProces
