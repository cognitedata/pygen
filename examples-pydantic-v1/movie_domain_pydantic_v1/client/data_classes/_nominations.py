from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from movie_domain_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["Nomination", "NominationApply", "NominationList"]


class Nomination(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None


class NominationApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    year: int

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("IntegrationTestsImmutable", "Nomination"),
            properties={
                "name": self.name,
                "year": self.year,
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []

        return dm.InstancesApply(nodes, edges)


class NominationList(TypeList[Nomination]):
    _NODE = Nomination
