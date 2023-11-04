from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

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
    """This represent a read version of reviewer.

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

    space: str = "IntegrationTestsImmutable"
    data_governance_role_type_id: Optional[str] = Field(None, alias="DataGovernanceRoleTypeID")
    name: Optional[str] = Field(None, alias="Name")
    organisation_id: Optional[str] = Field(None, alias="OrganisationID")
    role_type_id: Optional[str] = Field(None, alias="RoleTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")

    def as_apply(self) -> ReviewersApply:
        """Convert this read version of reviewer to a write version."""
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
    """This represent a write version of reviewer.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the reviewer.
        data_governance_role_type_id: The data governance role type id field.
        name: The name field.
        organisation_id: The organisation id field.
        role_type_id: The role type id field.
        workflow_persona_type_id: The workflow persona type id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    data_governance_role_type_id: Optional[str] = Field(None, alias="DataGovernanceRoleTypeID")
    name: Optional[str] = Field(None, alias="Name")
    organisation_id: Optional[str] = Field(None, alias="OrganisationID")
    role_type_id: Optional[str] = Field(None, alias="RoleTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Reviewers", "a7b641adc001b9"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ReviewersList(TypeList[Reviewers]):
    """List of reviewers in read version."""

    _NODE = Reviewers

    def as_apply(self) -> ReviewersApplyList:
        """Convert this read version of reviewer to a write version."""
        return ReviewersApplyList([node.as_apply() for node in self.data])


class ReviewersApplyList(TypeApplyList[ReviewersApply]):
    """List of reviewers in write version."""

    _NODE = ReviewersApply
