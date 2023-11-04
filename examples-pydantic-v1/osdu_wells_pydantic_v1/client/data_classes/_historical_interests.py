from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "HistoricalInterests",
    "HistoricalInterestsApply",
    "HistoricalInterestsList",
    "HistoricalInterestsApplyList",
    "HistoricalInterestsFields",
    "HistoricalInterestsTextFields",
]


HistoricalInterestsTextFields = Literal["effective_date_time", "interest_type_id", "termination_date_time"]
HistoricalInterestsFields = Literal["effective_date_time", "interest_type_id", "termination_date_time"]

_HISTORICALINTERESTS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "interest_type_id": "InterestTypeID",
    "termination_date_time": "TerminationDateTime",
}


class HistoricalInterests(DomainModel):
    """This represent a read version of historical interest.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the historical interest.
        effective_date_time: The effective date time field.
        interest_type_id: The interest type id field.
        termination_date_time: The termination date time field.
        created_time: The created time of the historical interest node.
        last_updated_time: The last updated time of the historical interest node.
        deleted_time: If present, the deleted time of the historical interest node.
        version: The version of the historical interest node.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> HistoricalInterestsApply:
        """Convert this read version of historical interest to a write version."""
        return HistoricalInterestsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            interest_type_id=self.interest_type_id,
            termination_date_time=self.termination_date_time,
        )


class HistoricalInterestsApply(DomainModelApply):
    """This represent a write version of historical interest.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the historical interest.
        effective_date_time: The effective date time field.
        interest_type_id: The interest type id field.
        termination_date_time: The termination date time field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.interest_type_id is not None:
            properties["InterestTypeID"] = self.interest_type_id
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "HistoricalInterests", "7399eff7364ba6"),
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


class HistoricalInterestsList(TypeList[HistoricalInterests]):
    """List of historical interests in read version."""

    _NODE = HistoricalInterests

    def as_apply(self) -> HistoricalInterestsApplyList:
        """Convert this read version of historical interest to a write version."""
        return HistoricalInterestsApplyList([node.as_apply() for node in self.data])


class HistoricalInterestsApplyList(TypeApplyList[HistoricalInterestsApply]):
    """List of historical interests in write version."""

    _NODE = HistoricalInterestsApply
