from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, TypeList

__all__ = ["Director", "DirectorApply", "DirectorList"]


class Director(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    won_oscar: Optional[bool] = Field(None, alias="wonOscar")
    person: Optional[str] = None
    movies: list[str] = []
    nomination: list[str] = []


class DirectorApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    won_oscar: Optional[bool] = None
    person: Optional[str] = None
    movies: list[str] = []
    nomination: list[str] = []

    def to_node(self) -> dm.NodeApply:
        return dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("IntegrationTestsImmutable", "Role"),
                    properties={
                        "wonOscar": self.won_oscar,
                        "person": self.person,
                    },
                ),
            ],
        )


class DirectorList(TypeList[Director]):
    _NODE = Director
