from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Rating", "RatingApply", "RatingList", "RatingApplyList"]


class Rating(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    score: Optional[str] = None
    votes: Optional[str] = None


class RatingApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    score: Optional[str] = None
    votes: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.score is not None:
            properties["score"] = self.score
        if self.votes is not None:
            properties["votes"] = self.votes
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Rating"),
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


class RatingList(TypeList[Rating]):
    _NODE = Rating


class RatingApplyList(TypeApplyList[RatingApply]):
    _NODE = RatingApply
