from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._movies import MovieApply
    from ._nominations import NominationApply
    from ._persons import PersonApply

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
    person: Optional[Union[str, "PersonApply"]] = None
    movies: list[Union[str, "MovieApply"]] = []
    nomination: list[Union[str, "NominationApply"]] = []

    def to_instances_apply(self) -> InstancesApply:
        this_node = dm.NodeApply(
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
        nodes = [this_node]
        edges = []
        return InstancesApply(nodes, edges)


class DirectorList(TypeList[Director]):
    _NODE = Director
