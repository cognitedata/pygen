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
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> HistoricalInterestsApply:
        return HistoricalInterestsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            interest_type_id=self.interest_type_id,
            termination_date_time=self.termination_date_time,
        )


class HistoricalInterestsApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.interest_type_id is not None:
            properties["InterestTypeID"] = self.interest_type_id
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "HistoricalInterests"),
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


class HistoricalInterestsList(TypeList[HistoricalInterests]):
    _NODE = HistoricalInterests

    def as_apply(self) -> HistoricalInterestsApplyList:
        return HistoricalInterestsApplyList([node.as_apply() for node in self.data])


class HistoricalInterestsApplyList(TypeApplyList[HistoricalInterestsApply]):
    _NODE = HistoricalInterestsApply
