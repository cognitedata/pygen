from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "BestLeadingActor",
    "BestLeadingActorApply",
    "BestLeadingActorList",
    "BestLeadingActorApplyList",
    "BestLeadingActorTextFields",
]


BestLeadingActorTextFields = Literal["name"]

_BESTLEADINGACTOR_TEXT_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class BestLeadingActor(DomainModel):
    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None

    def as_apply(self) -> BestLeadingActorApply:
        return BestLeadingActorApply(
            external_id=self.external_id,
            name=self.name,
            year=self.year,
        )


class BestLeadingActorApply(DomainModelApply):
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


class BestLeadingActorList(TypeList[BestLeadingActor]):
    _NODE = BestLeadingActor

    def as_apply(self) -> BestLeadingActorApplyList:
        return BestLeadingActorApplyList([node.as_apply() for node in self.data])


class BestLeadingActorApplyList(TypeApplyList[BestLeadingActorApply]):
    _NODE = BestLeadingActorApply
