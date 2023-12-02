from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._role import Role, RoleApply


__all__ = ["Person", "PersonApply", "PersonList", "PersonApplyList", "PersonFields", "PersonTextFields"]


PersonTextFields = Literal["name"]
PersonFields = Literal["birth_year", "name"]

_PERSON_PROPERTIES_BY_FIELD = {
    "birth_year": "birthYear",
    "name": "name",
}


class Person(DomainModel):
    """This represents the reading version of person.

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

    space: str = DEFAULT_INSTANCE_SPACE
    birth_year: Optional[int] = Field(None, alias="birthYear")
    name: Optional[str] = None
    roles: Union[list[Role], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> PersonApply:
        """Convert this read version of person to the writing version."""
        return PersonApply(
            space=self.space,
            external_id=self.external_id,
            birth_year=self.birth_year,
            name=self.name,
            roles=[role.as_apply() if isinstance(role, DomainModel) else role for role in self.roles or []],
        )


class PersonApply(DomainModelApply):
    """This represents the writing version of person.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the person.
        birth_year: The birth year field.
        name: The name field.
        roles: The role field.
        existing_version: Fail the ingestion request if the person version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    birth_year: Optional[int] = Field(None, alias="birthYear")
    name: str
    roles: Union[list[RoleApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Person", "2"
        )

        properties = {}
        if self.birth_year is not None:
            properties["birthYear"] = self.birth_year
        if self.name is not None:
            properties["name"] = self.name

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "Person.roles")
        for role in self.roles or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, role, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        return resources


class PersonList(DomainModelList[Person]):
    """List of persons in the read version."""

    _INSTANCE = Person

    def as_apply(self) -> PersonApplyList:
        """Convert these read versions of person to the writing versions."""
        return PersonApplyList([node.as_apply() for node in self.data])


class PersonApplyList(DomainModelApplyList[PersonApply]):
    """List of persons in the writing version."""

    _INSTANCE = PersonApply


def _create_person_filter(
    view_id: dm.ViewId,
    min_birth_year: int | None = None,
    max_birth_year: int | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_birth_year or max_birth_year:
        filters.append(dm.filters.Range(view_id.as_property_ref("birthYear"), gte=min_birth_year, lte=max_birth_year))
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
