from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "UnacceptableUsage",
    "UnacceptableUsageApply",
    "UnacceptableUsageList",
    "UnacceptableUsageApplyList",
    "UnacceptableUsageFields",
    "UnacceptableUsageTextFields",
]


UnacceptableUsageTextFields = Literal[
    "data_quality_id",
    "data_quality_rule_set_id",
    "value_chain_status_type_id",
    "workflow_persona_type_id",
    "workflow_usage_type_id",
]
UnacceptableUsageFields = Literal[
    "data_quality_id",
    "data_quality_rule_set_id",
    "value_chain_status_type_id",
    "workflow_persona_type_id",
    "workflow_usage_type_id",
]

_UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD = {
    "data_quality_id": "DataQualityID",
    "data_quality_rule_set_id": "DataQualityRuleSetID",
    "value_chain_status_type_id": "ValueChainStatusTypeID",
    "workflow_persona_type_id": "WorkflowPersonaTypeID",
    "workflow_usage_type_id": "WorkflowUsageTypeID",
}


class UnacceptableUsage(DomainModel):
    space: str = "IntegrationTestsImmutable"
    data_quality_id: Optional[str] = Field(None, alias="DataQualityID")
    data_quality_rule_set_id: Optional[str] = Field(None, alias="DataQualityRuleSetID")
    value_chain_status_type_id: Optional[str] = Field(None, alias="ValueChainStatusTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")
    workflow_usage_type_id: Optional[str] = Field(None, alias="WorkflowUsageTypeID")

    def as_apply(self) -> UnacceptableUsageApply:
        return UnacceptableUsageApply(
            external_id=self.external_id,
            data_quality_id=self.data_quality_id,
            data_quality_rule_set_id=self.data_quality_rule_set_id,
            value_chain_status_type_id=self.value_chain_status_type_id,
            workflow_persona_type_id=self.workflow_persona_type_id,
            workflow_usage_type_id=self.workflow_usage_type_id,
        )


class UnacceptableUsageApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    data_quality_id: Optional[str] = Field(None, alias="DataQualityID")
    data_quality_rule_set_id: Optional[str] = Field(None, alias="DataQualityRuleSetID")
    value_chain_status_type_id: Optional[str] = Field(None, alias="ValueChainStatusTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")
    workflow_usage_type_id: Optional[str] = Field(None, alias="WorkflowUsageTypeID")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.data_quality_id is not None:
            properties["DataQualityID"] = self.data_quality_id
        if self.data_quality_rule_set_id is not None:
            properties["DataQualityRuleSetID"] = self.data_quality_rule_set_id
        if self.value_chain_status_type_id is not None:
            properties["ValueChainStatusTypeID"] = self.value_chain_status_type_id
        if self.workflow_persona_type_id is not None:
            properties["WorkflowPersonaTypeID"] = self.workflow_persona_type_id
        if self.workflow_usage_type_id is not None:
            properties["WorkflowUsageTypeID"] = self.workflow_usage_type_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "UnacceptableUsage"),
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


class UnacceptableUsageList(TypeList[UnacceptableUsage]):
    _NODE = UnacceptableUsage

    def as_apply(self) -> UnacceptableUsageApplyList:
        return UnacceptableUsageApplyList([node.as_apply() for node in self.data])


class UnacceptableUsageApplyList(TypeApplyList[UnacceptableUsageApply]):
    _NODE = UnacceptableUsageApply
