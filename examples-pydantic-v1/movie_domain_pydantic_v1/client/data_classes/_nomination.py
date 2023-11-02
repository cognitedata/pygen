from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "Nomination",
    "NominationApply",
    "NominationList",
    "NominationApplyList",
    "NominationFields",
    "NominationTextFields",
]


NominationTextFields = Literal["name"]
NominationFields = Literal["name", "year"]

_NOMINATION_PROPERTIES_BY_FIELD = {
    "name": "name",
    "year": "year",
}


class Nomination(DomainModel):
    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None

    def as_apply(self) -> NominationApply:
        return NominationApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            year=self.year,
        )


class NominationApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    name: str
    year: int

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.year is not None:
            properties["year"] = self.year
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Nomination", "2"),
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


class NominationList(TypeList[Nomination]):
    _NODE = Nomination

    def as_apply(self) -> NominationApplyList:
        return NominationApplyList([node.as_apply() for node in self.data])


class NominationApplyList(TypeApplyList[NominationApply]):
    _NODE = NominationApply
