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
    """This represent a read version of rating.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rating.
        score: The score field.
        votes: The vote field.
        created_time: The created time of the rating node.
        last_updated_time: The last updated time of the rating node.
        deleted_time: If present, the deleted time of the rating node.
        version: The version of the rating node.
    """

    space: str = "IntegrationTestsImmutable"
    score: Optional[str] = None
    votes: Optional[str] = None

    def as_apply(self) -> RatingApply:
        """Convert this read version of rating to a write version."""
        return RatingApply(
            space=self.space,
            external_id=self.external_id,
            score=self.score,
            votes=self.votes,
        )


class RatingApply(DomainModelApply):
    """This represent a write version of rating.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rating.
        score: The score field.
        votes: The vote field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

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
    """List of ratings in read version."""

    _NODE = Rating

    def as_apply(self) -> RatingApplyList:
        """Convert this read version of rating to a write version."""
        return RatingApplyList([node.as_apply() for node in self.data])


class RatingApplyList(TypeApplyList[RatingApply]):
    """List of ratings in write version."""

    _NODE = RatingApply
