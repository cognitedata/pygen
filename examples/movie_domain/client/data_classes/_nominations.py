from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import CircularModelApply, DomainModel, TypeList

__all__ = ["Nomination", "NominationApply", "NominationList"]


class Nomination(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None


class NominationApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    year: int

    def to_node(self) -> dm.NodeApply:
        return dm.NodeApply(
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


class NominationList(TypeList[Nomination]):
    _NODE = Nomination
