from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._role import RoleApply

__all__ = ["Person", "PersonApply", "PersonList", "PersonApplyList", "PersonFields", "PersonTextFields"]


PersonTextFields = Literal["name"]
PersonFields = Literal["birth_year", "name"]

_PERSON_PROPERTIES_BY_FIELD = {
    "birth_year": "birthYear",
    "name": "name",
}


class Person(DomainModel):
    """This represent a read version of person.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the person.
        birth_year: The birth year field.
        name: The name field.
        roles: The role field.
        created_time: The created time of the person node.
        last_updated_time: The last updated time of the person node.
        deleted_time: If present, the deleted time of the person node.
        version: The version of the person node.
    """

    space: str = "IntegrationTestsImmutable"
    birth_year: Optional[int] = Field(None, alias="birthYear")
    name: Optional[str] = None
    roles: Optional[list[str]] = None

    def as_apply(self) -> PersonApply:
        """Convert this read version of person to a write version."""
        return PersonApply(
            space=self.space,
            external_id=self.external_id,
            birth_year=self.birth_year,
            name=self.name,
            roles=self.roles,
        )


class PersonApply(DomainModelApply):
    """This represent a write version of person.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the person.
        birth_year: The birth year field.
        name: The name field.
        roles: The role field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    birth_year: Optional[int] = Field(None, alias="birthYear")
    name: str
    roles: Union[list[RoleApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.birth_year is not None:
            properties["birthYear"] = self.birth_year
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
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
                instances = role._to_instances_apply(cache, view_by_write_class)
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
    """List of persons in read version."""

    _NODE = Person

    def as_apply(self) -> PersonApplyList:
        """Convert this read version of person to a write version."""
        return PersonApplyList([node.as_apply() for node in self.data])


class PersonApplyList(TypeApplyList[PersonApply]):
    """List of persons in write version."""

    _NODE = PersonApply
