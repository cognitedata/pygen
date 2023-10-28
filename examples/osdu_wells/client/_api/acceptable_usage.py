from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    AcceptableUsage,
    AcceptableUsageApply,
    AcceptableUsageList,
    AcceptableUsageApplyList,
    AcceptableUsageFields,
    AcceptableUsageTextFields,
)
from osdu_wells.client.data_classes._acceptable_usage import _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD


class AcceptableUsageAPI(TypeAPI[AcceptableUsage, AcceptableUsageApply, AcceptableUsageList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AcceptableUsage,
            class_apply_type=AcceptableUsageApply,
            class_list=AcceptableUsageList,
        )
        self._view_id = view_id

    def apply(
        self, acceptable_usage: AcceptableUsageApply | Sequence[AcceptableUsageApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(acceptable_usage, AcceptableUsageApply):
            instances = acceptable_usage.to_instances_apply()
        else:
            instances = AcceptableUsageApplyList(acceptable_usage).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> AcceptableUsage:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AcceptableUsageList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> AcceptableUsage | AcceptableUsageList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AcceptableUsageList:
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        group_by: AcceptableUsageFields | Sequence[AcceptableUsageFields] = None,
        query: str | None = None,
        search_properties: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        group_by: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        query: str | None = None,
        search_property: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AcceptableUsageFields,
        interval: float,
        query: str | None = None,
        search_property: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AcceptableUsageList:
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
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
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if data_quality_id and isinstance(data_quality_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("DataQualityID"), value=data_quality_id))
    if data_quality_id and isinstance(data_quality_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DataQualityID"), values=data_quality_id))
    if data_quality_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("DataQualityID"), value=data_quality_id_prefix))
    if data_quality_rule_set_id and isinstance(data_quality_rule_set_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DataQualityRuleSetID"), value=data_quality_rule_set_id)
        )
    if data_quality_rule_set_id and isinstance(data_quality_rule_set_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DataQualityRuleSetID"), values=data_quality_rule_set_id))
    if data_quality_rule_set_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("DataQualityRuleSetID"), value=data_quality_rule_set_id_prefix)
        )
    if value_chain_status_type_id and isinstance(value_chain_status_type_id, str):
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
    if workflow_usage_type_id and isinstance(workflow_usage_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("WorkflowUsageTypeID"), value=workflow_usage_type_id))
    if workflow_usage_type_id and isinstance(workflow_usage_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WorkflowUsageTypeID"), values=workflow_usage_type_id))
    if workflow_usage_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("WorkflowUsageTypeID"), value=workflow_usage_type_id_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
