from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList

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
    roles: Union[list[RoleApply], list[str]] = Field(default_factory=list, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.birth_year is not None:
            properties["birthYear"] = self.birth_year
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Person"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for role in self.roles:
            edge = self._create_role_edge(role)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(role, DomainModelApply):
                instances = role._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_role_edge(self, role: Union[str, RoleApply]) -> dm.EdgeApply:
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
