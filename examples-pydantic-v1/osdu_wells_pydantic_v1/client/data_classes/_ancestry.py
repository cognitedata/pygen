from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Ancestry", "AncestryApply", "AncestryList", "AncestryApplyList", "AncestryFields", "AncestryTextFields"]


AncestryTextFields = Literal["parents"]
AncestryFields = Literal["parents"]

_ANCESTRY_PROPERTIES_BY_FIELD = {
    "parents": "parents",
}


class Ancestry(DomainModel):
    space: str = "IntegrationTestsImmutable"
    parents: Optional[list[str]] = None

    def as_apply(self) -> AncestryApply:
        return AncestryApply(
            space=self.space,
            external_id=self.external_id,
            parents=self.parents,
        )


class AncestryApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    parents: Optional[list[str]] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.parents is not None:
            properties["parents"] = self.parents
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Ancestry"),
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


class AncestryList(TypeList[Ancestry]):
    _NODE = Ancestry

    def as_apply(self) -> AncestryApplyList:
        return AncestryApplyList([node.as_apply() for node in self.data])


class AncestryApplyList(TypeApplyList[AncestryApply]):
    _NODE = AncestryApply
