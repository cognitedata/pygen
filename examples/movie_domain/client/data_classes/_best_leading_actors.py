from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

__all__ = ["BestLeadingActor", "BestLeadingActorApply", "BestLeadingActorList"]


class BestLeadingActor(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None


class BestLeadingActorApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    year: int

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])
        node_data = dm.NodeOrEdgeData(
            source=dm.ContainerId("IntegrationTestsImmutable", "Nomination"),
            properties={
                "name": self.name,
                "year": self.year,
            },
        )

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[node_data],
        )
        nodes = [this_node]
        edges = []

        return InstancesApply(nodes, edges)


class BestLeadingActorList(TypeList[BestLeadingActor]):
    _NODE = BestLeadingActor
