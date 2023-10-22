from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._role import RoleApply

__all__ = ["Person", "PersonApply", "PersonList", "PersonApplyList", "PersonFields", "PersonTextFields"]


PersonTextFields = Literal["name"]
PersonFields = Literal["name", "birth_year"]

_PERSON_PROPERTIES_BY_FIELD = {
    "birth_year": "birthYear",
    "name": "name",
}


class Person(DomainModel):
    space: str = "IntegrationTestsImmutable"
    birth_year: Optional[int] = Field(None, alias="birthYear")
    name: Optional[str] = None
    roles: Optional[list[str]] = None

    def as_apply(self) -> PersonApply:
        return PersonApply(
            external_id=self.external_id,
            birth_year=self.birth_year,
            name=self.name,
            roles=self.roles,
        )


class PersonApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    birth_year: Optional[int] = None
    name: str
    roles: Union[list[RoleApply], list[str], None] = Field(default=None, repr=False)

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

        for role in self.roles or []:
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

    def as_apply(self) -> PersonApplyList:
        return PersonApplyList([node.as_apply() for node in self.data])


class PersonApplyList(TypeApplyList[PersonApply]):
    _NODE = PersonApply
