from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Legal", "LegalApply", "LegalList", "LegalApplyList", "LegalFields", "LegalTextFields"]


LegalTextFields = Literal["legaltags", "other_relevant_data_countries", "status"]
LegalFields = Literal["legaltags", "other_relevant_data_countries", "status"]

_LEGAL_PROPERTIES_BY_FIELD = {
    "legaltags": "legaltags",
    "other_relevant_data_countries": "otherRelevantDataCountries",
    "status": "status",
}


class Legal(DomainModel):
    space: str = "IntegrationTestsImmutable"
    legaltags: Optional[list[str]] = None
    other_relevant_data_countries: Optional[list[str]] = Field(None, alias="otherRelevantDataCountries")
    status: Optional[str] = None

    def as_apply(self) -> LegalApply:
        return LegalApply(
            space=self.space,
            external_id=self.external_id,
            legaltags=self.legaltags,
            other_relevant_data_countries=self.other_relevant_data_countries,
            status=self.status,
        )


class LegalApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    legaltags: Optional[list[str]] = None
    other_relevant_data_countries: Optional[list[str]] = Field(None, alias="otherRelevantDataCountries")
    status: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.legaltags is not None:
            properties["legaltags"] = self.legaltags
        if self.other_relevant_data_countries is not None:
            properties["otherRelevantDataCountries"] = self.other_relevant_data_countries
        if self.status is not None:
            properties["status"] = self.status
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Legal"),
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


class LegalList(TypeList[Legal]):
    _NODE = Legal

    def as_apply(self) -> LegalApplyList:
        return LegalApplyList([node.as_apply() for node in self.data])


class LegalApplyList(TypeApplyList[LegalApply]):
    _NODE = LegalApply
