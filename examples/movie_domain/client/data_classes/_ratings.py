from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

__all__ = ["Rating", "RatingApply", "RatingList"]


class Rating(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    score: Optional[str] = None
    vote: Optional[str] = Field(None, alias="votes")


class RatingApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    score: Optional[str] = None
    vote: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("IntegrationTestsImmutable", "Rating"),
            properties={
                "score": self.score,
                "votes": self.vote,
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

        return InstancesApply(nodes, edges)


class RatingList(TypeList[Rating]):
    _NODE = Rating
