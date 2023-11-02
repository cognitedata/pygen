from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Rating", "RatingApply", "RatingList", "RatingApplyList", "RatingFields", "RatingTextFields"]


RatingTextFields = Literal["score", "votes"]
RatingFields = Literal["score", "votes"]

_RATING_PROPERTIES_BY_FIELD = {
    "score": "score",
    "votes": "votes",
}


class Rating(DomainModel):
    space: str = "IntegrationTestsImmutable"
    score: Optional[str] = None
    votes: Optional[str] = None

    def as_apply(self) -> RatingApply:
        return RatingApply(
            space=self.space,
            external_id=self.external_id,
            score=self.score,
            votes=self.votes,
        )


class RatingApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    score: Optional[str] = None
    votes: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.score is not None:
            properties["score"] = self.score
        if self.votes is not None:
            properties["votes"] = self.votes
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Rating", "2"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class RatingList(TypeList[Rating]):
    _NODE = Rating

    def as_apply(self) -> RatingApplyList:
        return RatingApplyList([node.as_apply() for node in self.data])


class RatingApplyList(TypeApplyList[RatingApply]):
    _NODE = RatingApply
