from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._roles import RoleApply

__all__ = ["Person", "PersonApply", "PersonList"]


class Person(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    birth_year: Optional[int] = Field(None, alias="birthYear")
    name: Optional[str] = None
    roles: list[str] = []


class PersonApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    birth_year: Optional[int] = None
    name: str
    roles: list[Union[str, "RoleApply"]] = Field(default_factory=lambda: [], repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("IntegrationTestsImmutable", "Person"),
            properties={
                "birthYear": self.birth_year,
                "name": self.name,
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

        for role in self.roles:
            edge = self._create_role_edge(role)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(role, DomainModelApply):
                instances = role._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_role_edge(self, role: Union[str, "RoleApply"]) -> dm.EdgeApply:
        if isinstance(role, str):
            end_node_ext_id = role
        elif isinstance(role, DomainModelApply):
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
