from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

__all__ = ["BestLeadingActress", "BestLeadingActressApply", "BestLeadingActressList"]


class BestLeadingActress(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None


class BestLeadingActressApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    year: int

    def to_instances_apply(self) -> InstancesApply:
        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("IntegrationTestsImmutable", "Nomination"),
                    properties={
                        "name": self.name,
                        "year": self.year,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []
        return InstancesApply(nodes, edges)


class BestLeadingActressList(TypeList[BestLeadingActress]):
    _NODE = BestLeadingActress
