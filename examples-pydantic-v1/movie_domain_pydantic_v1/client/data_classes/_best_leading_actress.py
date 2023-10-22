from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "BestLeadingActress",
    "BestLeadingActressApply",
    "BestLeadingActressList",
    "BestLeadingActressApplyList",
    "BestLeadingActressTextFields",
]


BestLeadingActressTextFields = Literal["name"]

_BESTLEADINGACTRESS_TEXT_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class BestLeadingActress(DomainModel):
    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None

    def as_apply(self) -> BestLeadingActressApply:
        return BestLeadingActressApply(
            external_id=self.external_id,
            name=self.name,
            year=self.year,
        )


class BestLeadingActressApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
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

    def as_apply(self) -> BestLeadingActressApplyList:
        return BestLeadingActressApplyList([node.as_apply() for node in self.data])


class BestLeadingActressApplyList(TypeApplyList[BestLeadingActressApply]):
    _NODE = BestLeadingActressApply
