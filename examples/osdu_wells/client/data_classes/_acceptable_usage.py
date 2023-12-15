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
    """This represents the reading version of acceptable usage.

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

    space: str = DEFAULT_INSTANCE_SPACE
    data_quality_id: Optional[str] = Field(None, alias="DataQualityID")
    data_quality_rule_set_id: Optional[str] = Field(None, alias="DataQualityRuleSetID")
    value_chain_status_type_id: Optional[str] = Field(None, alias="ValueChainStatusTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")
    workflow_usage_type_id: Optional[str] = Field(None, alias="WorkflowUsageTypeID")

    def as_apply(self) -> AcceptableUsageApply:
        """Convert this read version of acceptable usage to the writing version."""
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
    """This represents the writing version of acceptable usage.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the acceptable usage.
        data_quality_id: The data quality id field.
        data_quality_rule_set_id: The data quality rule set id field.
        value_chain_status_type_id: The value chain status type id field.
        workflow_persona_type_id: The workflow persona type id field.
        workflow_usage_type_id: The workflow usage type id field.
        existing_version: Fail the ingestion request if the acceptable usage version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    data_quality_id: Optional[str] = Field(None, alias="DataQualityID")
    data_quality_rule_set_id: Optional[str] = Field(None, alias="DataQualityRuleSetID")
    value_chain_status_type_id: Optional[str] = Field(None, alias="ValueChainStatusTypeID")
    workflow_persona_type_id: Optional[str] = Field(None, alias="WorkflowPersonaTypeID")
    workflow_usage_type_id: Optional[str] = Field(None, alias="WorkflowUsageTypeID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "AcceptableUsage", "d7e8986cd55d22"
        )

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


class AcceptableUsageList(DomainModelList[AcceptableUsage]):
    """List of acceptable usages in the read version."""

    _INSTANCE = AcceptableUsage

    def as_apply(self) -> AcceptableUsageApplyList:
        """Convert these read versions of acceptable usage to the writing versions."""
        return AcceptableUsageApplyList([node.as_apply() for node in self.data])


class AcceptableUsageApplyList(DomainModelApplyList[AcceptableUsageApply]):
    """List of acceptable usages in the writing version."""

    _INSTANCE = AcceptableUsageApply


def _create_acceptable_usage_filter(
    view_id: dm.ViewId,
    data_quality_id: str | list[str] | None = None,
    data_quality_id_prefix: str | None = None,
    data_quality_rule_set_id: str | list[str] | None = None,
    data_quality_rule_set_id_prefix: str | None = None,
    value_chain_status_type_id: str | list[str] | None = None,
    value_chain_status_type_id_prefix: str | None = None,
    workflow_persona_type_id: str | list[str] | None = None,
    workflow_persona_type_id_prefix: str | None = None,
    workflow_usage_type_id: str | list[str] | None = None,
    workflow_usage_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if data_quality_id is not None and isinstance(data_quality_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("DataQualityID"), value=data_quality_id))
    if data_quality_id and isinstance(data_quality_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DataQualityID"), values=data_quality_id))
    if data_quality_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("DataQualityID"), value=data_quality_id_prefix))
    if data_quality_rule_set_id is not None and isinstance(data_quality_rule_set_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DataQualityRuleSetID"), value=data_quality_rule_set_id)
        )
    if data_quality_rule_set_id and isinstance(data_quality_rule_set_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DataQualityRuleSetID"), values=data_quality_rule_set_id))
    if data_quality_rule_set_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("DataQualityRuleSetID"), value=data_quality_rule_set_id_prefix)
        )
    if value_chain_status_type_id is not None and isinstance(value_chain_status_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ValueChainStatusTypeID"), value=value_chain_status_type_id)
        )
    if value_chain_status_type_id and isinstance(value_chain_status_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("ValueChainStatusTypeID"), values=value_chain_status_type_id)
        )
    if value_chain_status_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ValueChainStatusTypeID"), value=value_chain_status_type_id_prefix
            )
        )
    if workflow_persona_type_id is not None and isinstance(workflow_persona_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id)
        )
    if workflow_persona_type_id and isinstance(workflow_persona_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WorkflowPersonaTypeID"), values=workflow_persona_type_id))
    if workflow_persona_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id_prefix)
        )
    if workflow_usage_type_id is not None and isinstance(workflow_usage_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("WorkflowUsageTypeID"), value=workflow_usage_type_id))
    if workflow_usage_type_id and isinstance(workflow_usage_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WorkflowUsageTypeID"), values=workflow_usage_type_id))
    if workflow_usage_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("WorkflowUsageTypeID"), value=workflow_usage_type_id_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
