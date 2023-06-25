from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

__all__ = ["Rating", "RatingApply", "RatingList"]


class Rating(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    score: Optional[str] = None
    votes: Optional[str] = None


class RatingApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    score: Optional[str] = None
    votes: Optional[str] = None

    def to_instances_apply(self) -> InstancesApply:
        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("IntegrationTestsImmutable", "Rating"),
                    properties={
                        "score": self.score,
                        "votes": self.votes,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []
        return InstancesApply(nodes, edges)


class RatingList(TypeList[Rating]):
    _NODE = Rating
