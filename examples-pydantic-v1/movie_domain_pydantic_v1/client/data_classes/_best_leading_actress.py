from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["BestLeadingActress", "BestLeadingActressApply", "BestLeadingActressList", "BestLeadingActressApplyList"]


class BestLeadingActress(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None


class BestLeadingActressApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    year: int

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.year is not None:
            properties["year"] = self.year
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Nomination"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class BestLeadingActressList(TypeList[BestLeadingActress]):
    _NODE = BestLeadingActress


class BestLeadingActressApplyList(TypeApplyList[BestLeadingActressApply]):
    _NODE = BestLeadingActressApply
