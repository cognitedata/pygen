from __future__ import annotations

from typing import Literal, Optional

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


__all__ = [
    "Reviewers",
    "ReviewersApply",
    "ReviewersList",
    "ReviewersApplyList",
    "ReviewersFields",
    "ReviewersTextFields",
]


ReviewersTextFields = Literal[
    "data_governance_role_type_id", "name", "organisation_id", "role_type_id", "workflow_persona_type_id"
]
ReviewersFields = Literal[
    "data_governance_role_type_id", "name", "organisation_id", "role_type_id", "workflow_persona_type_id"
]

_REVIEWERS_PROPERTIES_BY_FIELD = {
    "data_governance_role_type_id": "DataGovernanceRoleTypeID",
    "name": "Name",
    "organisation_id": "OrganisationID",
    "role_type_id": "RoleTypeID",
    "workflow_persona_type_id": "WorkflowPersonaTypeID",
}


class Reviewers(DomainModel):
    """This represents the reading version of reviewer.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the reviewer.
        data_governance_role_type_id: The data governance role type id field.
        name: The name field.
        organisation_id: The organisation id field.
        role_type_id: The role type id field.
        workflow_persona_type_id: The workflow persona type id field.
        created_time: The created time of the reviewer node.
        last_updated_time: The last updated time of the reviewer node.
        deleted_time: If present, the deleted time of the reviewer node.
        version: The version of the reviewer node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    data_governance_role_type_id: Optional[str] = Field(None, alias="DataGovernanceRoleTypeID")
    name: Optional[str] = Field(None, alias="Name")
    organisation_id: Optional[str] = Field(None, alias="OrganisationID")
    role_type_id: Optional[str] = Field(None, alias="RoleTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")

    def as_apply(self) -> ReviewersApply:
        """Convert this read version of reviewer to the writing version."""
        return ReviewersApply(
            space=self.space,
            external_id=self.external_id,
            data_governance_role_type_id=self.data_governance_role_type_id,
            name=self.name,
            organisation_id=self.organisation_id,
            role_type_id=self.role_type_id,
            workflow_persona_type_id=self.workflow_persona_type_id,
        )


class ReviewersApply(DomainModelApply):
    """This represents the writing version of reviewer.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the reviewer.
        data_governance_role_type_id: The data governance role type id field.
        name: The name field.
        organisation_id: The organisation id field.
        role_type_id: The role type id field.
        workflow_persona_type_id: The workflow persona type id field.
        existing_version: Fail the ingestion request if the reviewer version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    data_governance_role_type_id: Optional[str] = Field(None, alias="DataGovernanceRoleTypeID")
    name: Optional[str] = Field(None, alias="Name")
    organisation_id: Optional[str] = Field(None, alias="OrganisationID")
    role_type_id: Optional[str] = Field(None, alias="RoleTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Reviewers", "a7b641adc001b9"
        )

        properties = {}
        if self.data_governance_role_type_id is not None:
            properties["DataGovernanceRoleTypeID"] = self.data_governance_role_type_id
        if self.name is not None:
            properties["Name"] = self.name
        if self.organisation_id is not None:
            properties["OrganisationID"] = self.organisation_id
        if self.role_type_id is not None:
            properties["RoleTypeID"] = self.role_type_id
        if self.workflow_persona_type_id is not None:
            properties["WorkflowPersonaTypeID"] = self.workflow_persona_type_id

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

        return resources


class ReviewersList(DomainModelList[Reviewers]):
    """List of reviewers in the read version."""

    _INSTANCE = Reviewers

    def as_apply(self) -> ReviewersApplyList:
        """Convert these read versions of reviewer to the writing versions."""
        return ReviewersApplyList([node.as_apply() for node in self.data])


class ReviewersApplyList(DomainModelApplyList[ReviewersApply]):
    """List of reviewers in the writing version."""

    _INSTANCE = ReviewersApply


def _create_reviewer_filter(
    view_id: dm.ViewId,
    data_governance_role_type_id: str | list[str] | None = None,
    data_governance_role_type_id_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    organisation_id: str | list[str] | None = None,
    organisation_id_prefix: str | None = None,
    role_type_id: str | list[str] | None = None,
    role_type_id_prefix: str | None = None,
    workflow_persona_type_id: str | list[str] | None = None,
    workflow_persona_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if data_governance_role_type_id and isinstance(data_governance_role_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DataGovernanceRoleTypeID"), value=data_governance_role_type_id)
        )
    if data_governance_role_type_id and isinstance(data_governance_role_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("DataGovernanceRoleTypeID"), values=data_governance_role_type_id)
        )
    if data_governance_role_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DataGovernanceRoleTypeID"), value=data_governance_role_type_id_prefix
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Name"), value=name_prefix))
    if organisation_id and isinstance(organisation_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("OrganisationID"), value=organisation_id))
    if organisation_id and isinstance(organisation_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("OrganisationID"), values=organisation_id))
    if organisation_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("OrganisationID"), value=organisation_id_prefix))
    if role_type_id and isinstance(role_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RoleTypeID"), value=role_type_id))
    if role_type_id and isinstance(role_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RoleTypeID"), values=role_type_id))
    if role_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RoleTypeID"), value=role_type_id_prefix))
    if workflow_persona_type_id and isinstance(workflow_persona_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id)
        )
    if workflow_persona_type_id and isinstance(workflow_persona_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WorkflowPersonaTypeID"), values=workflow_persona_type_id))
    if workflow_persona_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
