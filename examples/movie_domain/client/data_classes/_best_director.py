from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "BestDirector",
    "BestDirectorApply",
    "BestDirectorList",
    "BestDirectorApplyList",
    "BestDirectorFields",
    "BestDirectorTextFields",
]


BestDirectorTextFields = Literal["name"]
BestDirectorFields = Literal["name", "year"]

_BESTDIRECTOR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "year": "year",
}


class BestDirector(DomainModel):
    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None

    def as_apply(self) -> BestDirectorApply:
        return BestDirectorApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            year=self.year,
        )


class BestDirectorApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    name: str
    year: int

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.year is not None:
            properties["year"] = self.year
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "BestDirector", "2"),
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


class BestDirectorList(TypeList[BestDirector]):
    _NODE = BestDirector

    def as_apply(self) -> BestDirectorApplyList:
        return BestDirectorApplyList([node.as_apply() for node in self.data])


class BestDirectorApplyList(TypeApplyList[BestDirectorApply]):
    _NODE = BestDirectorApply
