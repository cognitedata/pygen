from __future__ import annotations

from typing import Literal, Optional, Union  # noqa: F401

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = ["Rating", "RatingApply", "RatingList", "RatingApplyList", "RatingFields", "RatingTextFields"]


RatingTextFields = Literal["score", "votes"]
RatingFields = Literal["score", "votes"]

_RATING_PROPERTIES_BY_FIELD = {
    "score": "score",
    "votes": "votes",
}


class Rating(DomainModel):
    """This represents the reading version of rating.

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

    space: str = DEFAULT_INSTANCE_SPACE
    score: Union[TimeSeries, str, None] = None
    votes: Union[TimeSeries, str, None] = None

    def as_apply(self) -> RatingApply:
        """Convert this read version of rating to the writing version."""
        return RatingApply(
            space=self.space,
            external_id=self.external_id,
            score=self.score,
            votes=self.votes,
        )


class RatingApply(DomainModelApply):
    """This represents the writing version of rating.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the rating.
        score: The score field.
        votes: The vote field.
        existing_version: Fail the ingestion request if the rating version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    score: Union[TimeSeries, str, None] = None
    votes: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Rating", "2"
        )

        properties = {}
        if self.score is not None:
            properties["score"] = self.score if isinstance(self.score, str) else self.score.external_id
        if self.votes is not None:
            properties["votes"] = self.votes if isinstance(self.votes, str) else self.votes.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.score, CogniteTimeSeries):
            resources.time_series.append(self.score)

        if isinstance(self.votes, CogniteTimeSeries):
            resources.time_series.append(self.votes)

        return resources


class RatingList(DomainModelList[Rating]):
    """List of ratings in the read version."""

    _INSTANCE = Rating

    def as_apply(self) -> RatingApplyList:
        """Convert these read versions of rating to the writing versions."""
        return RatingApplyList([node.as_apply() for node in self.data])


class RatingApplyList(DomainModelApplyList[RatingApply]):
    """List of ratings in the writing version."""

    _INSTANCE = RatingApply


def _create_rating_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
