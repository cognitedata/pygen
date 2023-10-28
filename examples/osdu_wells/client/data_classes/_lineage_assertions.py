from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "LineageAssertions",
    "LineageAssertionsApply",
    "LineageAssertionsList",
    "LineageAssertionsApplyList",
    "LineageAssertionsFields",
    "LineageAssertionsTextFields",
]


LineageAssertionsTextFields = Literal["id", "lineage_relationship_type"]
LineageAssertionsFields = Literal["id", "lineage_relationship_type"]

_LINEAGEASSERTIONS_PROPERTIES_BY_FIELD = {
    "id": "ID",
    "lineage_relationship_type": "LineageRelationshipType",
}


class LineageAssertions(DomainModel):
    space: str = "IntegrationTestsImmutable"
    id: Optional[str] = Field(None, alias="ID")
    lineage_relationship_type: Optional[str] = Field(None, alias="LineageRelationshipType")

    def as_apply(self) -> LineageAssertionsApply:
        return LineageAssertionsApply(
            external_id=self.external_id,
            id=self.id,
            lineage_relationship_type=self.lineage_relationship_type,
        )


class LineageAssertionsApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    id: Optional[str] = Field(None, alias="ID")
    lineage_relationship_type: Optional[str] = Field(None, alias="LineageRelationshipType")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.id is not None:
            properties["ID"] = self.id
        if self.lineage_relationship_type is not None:
            properties["LineageRelationshipType"] = self.lineage_relationship_type
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "LineageAssertions"),
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


class LineageAssertionsList(TypeList[LineageAssertions]):
    _NODE = LineageAssertions

    def as_apply(self) -> LineageAssertionsApplyList:
        return LineageAssertionsApplyList([node.as_apply() for node in self.data])


class LineageAssertionsApplyList(TypeApplyList[LineageAssertionsApply]):
    _NODE = LineageAssertionsApply
