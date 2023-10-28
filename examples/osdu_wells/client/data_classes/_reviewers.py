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
    space: str = "IntegrationTestsImmutable"
    data_governance_role_type_id: Optional[str] = Field(None, alias="DataGovernanceRoleTypeID")
    name: Optional[str] = Field(None, alias="Name")
    organisation_id: Optional[str] = Field(None, alias="OrganisationID")
    role_type_id: Optional[str] = Field(None, alias="RoleTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")

    def as_apply(self) -> ReviewersApply:
        return ReviewersApply(
            external_id=self.external_id,
            data_governance_role_type_id=self.data_governance_role_type_id,
            name=self.name,
            organisation_id=self.organisation_id,
            role_type_id=self.role_type_id,
            workflow_persona_type_id=self.workflow_persona_type_id,
        )


class ReviewersApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    data_governance_role_type_id: Optional[str] = Field(None, alias="DataGovernanceRoleTypeID")
    name: Optional[str] = Field(None, alias="Name")
    organisation_id: Optional[str] = Field(None, alias="OrganisationID")
    role_type_id: Optional[str] = Field(None, alias="RoleTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
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
                source=dm.ContainerId("IntegrationTestsImmutable", "Reviewers"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ReviewersList(TypeList[Reviewers]):
    _NODE = Reviewers

    def as_apply(self) -> ReviewersApplyList:
        return ReviewersApplyList([node.as_apply() for node in self.data])


class ReviewersApplyList(TypeApplyList[ReviewersApply]):
    _NODE = ReviewersApply
