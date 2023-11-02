from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    DrillingReasons,
    DrillingReasonsApply,
    DrillingReasonsList,
    DrillingReasonsApplyList,
    DrillingReasonsFields,
    DrillingReasonsTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._drilling_reasons import _DRILLINGREASONS_PROPERTIES_BY_FIELD


class DrillingReasonsAPI(TypeAPI[DrillingReasons, DrillingReasonsApply, DrillingReasonsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[DrillingReasonsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DrillingReasons,
            class_apply_type=DrillingReasonsApply,
            class_list=DrillingReasonsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, drilling_reason: DrillingReasonsApply | Sequence[DrillingReasonsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(drilling_reason, DrillingReasonsApply):
            instances = drilling_reason.to_instances_apply(self._view_by_write_class)
        else:
            instances = DrillingReasonsApplyList(drilling_reason).to_instances_apply(self._view_by_write_class)
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
    def retrieve(self, external_id: str) -> DrillingReasons:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DrillingReasonsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DrillingReasons | DrillingReasonsList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DrillingReasonsList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _DRILLINGREASONS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
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
        property: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        group_by: DrillingReasonsFields | Sequence[DrillingReasonsFields] = None,
        query: str | None = None,
        search_properties: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
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
        property: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        group_by: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        query: str | None = None,
        search_property: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _DRILLINGREASONS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DrillingReasonsFields,
        interval: float,
        query: str | None = None,
        search_property: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _DRILLINGREASONS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DrillingReasonsList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    lahee_class_id: str | list[str] | None = None,
    lahee_class_id_prefix: str | None = None,
    remark: str | list[str] | None = None,
    remark_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if effective_date_time and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if lahee_class_id and isinstance(lahee_class_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("LaheeClassID"), value=lahee_class_id))
    if lahee_class_id and isinstance(lahee_class_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("LaheeClassID"), values=lahee_class_id))
    if lahee_class_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("LaheeClassID"), value=lahee_class_id_prefix))
    if remark and isinstance(remark, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Remark"), value=remark))
    if remark and isinstance(remark, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Remark"), values=remark))
    if remark_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Remark"), value=remark_prefix))
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
