from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._roles import RoleApply

__all__ = ["Person", "PersonApply", "PersonList"]


class Person(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: Optional[str] = None
    birth_year: Optional[int] = Field(None, alias="birthYear")
    roles: list[str] = []


class PersonApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    name: str
    birth_year: Optional[int] = None
    roles: list[Union[str, "RoleApply"]] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("IntegrationTestsImmutable", "Person"),
                    properties={
                        "name": self.name,
                        "birthYear": self.birth_year,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []
        for role in self.roles:
            edge = self._create_role_edge(role)
            edges.append(edge)

            if isinstance(role, CircularModelApply):
                instances = role._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_role_edge(self, role: Union[str, "RoleApply"]) -> dm.EdgeApply:
        if isinstance(role, str):
            end_node_ext_id = role
        elif isinstance(role, CircularModelApply):
            end_node_ext_id = role.external_id
        else:
            raise TypeError(f"Expected str or RoleApply, got {type(role)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Person.roles"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class PersonList(TypeList[Person]):
    _NODE = Person
