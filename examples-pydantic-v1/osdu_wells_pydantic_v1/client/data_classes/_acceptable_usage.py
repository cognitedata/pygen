from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "AcceptableUsage",
    "AcceptableUsageApply",
    "AcceptableUsageList",
    "AcceptableUsageApplyList",
    "AcceptableUsageFields",
    "AcceptableUsageTextFields",
]


AcceptableUsageTextFields = Literal[
    "data_quality_id",
    "data_quality_rule_set_id",
    "value_chain_status_type_id",
    "workflow_persona_type_id",
    "workflow_usage_type_id",
]
AcceptableUsageFields = Literal[
    "data_quality_id",
    "data_quality_rule_set_id",
    "value_chain_status_type_id",
    "workflow_persona_type_id",
    "workflow_usage_type_id",
]

_ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD = {
    "data_quality_id": "DataQualityID",
    "data_quality_rule_set_id": "DataQualityRuleSetID",
    "value_chain_status_type_id": "ValueChainStatusTypeID",
    "workflow_persona_type_id": "WorkflowPersonaTypeID",
    "workflow_usage_type_id": "WorkflowUsageTypeID",
}


class AcceptableUsage(DomainModel):
    """This represent a read version of acceptable usage.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the acceptable usage.
        data_quality_id: The data quality id field.
        data_quality_rule_set_id: The data quality rule set id field.
        value_chain_status_type_id: The value chain status type id field.
        workflow_persona_type_id: The workflow persona type id field.
        workflow_usage_type_id: The workflow usage type id field.
        created_time: The created time of the acceptable usage node.
        last_updated_time: The last updated time of the acceptable usage node.
        deleted_time: If present, the deleted time of the acceptable usage node.
        version: The version of the acceptable usage node.
    """

    space: str = "IntegrationTestsImmutable"
    data_quality_id: Optional[str] = Field(None, alias="DataQualityID")
    data_quality_rule_set_id: Optional[str] = Field(None, alias="DataQualityRuleSetID")
    value_chain_status_type_id: Optional[str] = Field(None, alias="ValueChainStatusTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")
    workflow_usage_type_id: Optional[str] = Field(None, alias="WorkflowUsageTypeID")

    def as_apply(self) -> AcceptableUsageApply:
        """Convert this read version of acceptable usage to a write version."""
        return AcceptableUsageApply(
            space=self.space,
            external_id=self.external_id,
            data_quality_id=self.data_quality_id,
            data_quality_rule_set_id=self.data_quality_rule_set_id,
            value_chain_status_type_id=self.value_chain_status_type_id,
            workflow_persona_type_id=self.workflow_persona_type_id,
            workflow_usage_type_id=self.workflow_usage_type_id,
        )


class AcceptableUsageApply(DomainModelApply):
    """This represent a write version of acceptable usage.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the acceptable usage.
        data_quality_id: The data quality id field.
        data_quality_rule_set_id: The data quality rule set id field.
        value_chain_status_type_id: The value chain status type id field.
        workflow_persona_type_id: The workflow persona type id field.
        workflow_usage_type_id: The workflow usage type id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    data_quality_id: Optional[str] = Field(None, alias="DataQualityID")
    data_quality_rule_set_id: Optional[str] = Field(None, alias="DataQualityRuleSetID")
    value_chain_status_type_id: Optional[str] = Field(None, alias="ValueChainStatusTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")
    workflow_usage_type_id: Optional[str] = Field(None, alias="WorkflowUsageTypeID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "AcceptableUsage", "d7e8986cd55d22"),
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


class AcceptableUsageList(TypeList[AcceptableUsage]):
    """List of acceptable usages in read version."""

    _NODE = AcceptableUsage

    def as_apply(self) -> AcceptableUsageApplyList:
        """Convert this read version of acceptable usage to a write version."""
        return AcceptableUsageApplyList([node.as_apply() for node in self.data])


class AcceptableUsageApplyList(TypeApplyList[AcceptableUsageApply]):
    """List of acceptable usages in write version."""

    _NODE = AcceptableUsageApply
