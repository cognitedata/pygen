from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    FacilityEvents,
    FacilityEventsApply,
    FacilityEventsList,
    FacilityEventsApplyList,
    FacilityEventsFields,
    FacilityEventsTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._facility_events import _FACILITYEVENTS_PROPERTIES_BY_FIELD


class FacilityEventsAPI(TypeAPI[FacilityEvents, FacilityEventsApply, FacilityEventsList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FacilityEvents,
            class_apply_type=FacilityEventsApply,
            class_list=FacilityEventsList,
        )
        self._view_id = view_id

    def apply(
        self, facility_event: FacilityEventsApply | Sequence[FacilityEventsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(facility_event, FacilityEventsApply):
            instances = facility_event.to_instances_apply()
        else:
            instances = FacilityEventsApplyList(facility_event).to_instances_apply()
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
    def retrieve(self, external_id: str) -> FacilityEvents:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FacilityEventsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> FacilityEvents | FacilityEventsList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: FacilityEventsTextFields | Sequence[FacilityEventsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_event_type_id: str | list[str] | None = None,
        facility_event_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilityEventsList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_event_type_id,
            facility_event_type_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _FACILITYEVENTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: FacilityEventsFields | Sequence[FacilityEventsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FacilityEventsTextFields | Sequence[FacilityEventsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_event_type_id: str | list[str] | None = None,
        facility_event_type_id_prefix: str | None = None,
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
        property: FacilityEventsFields | Sequence[FacilityEventsFields] | None = None,
        group_by: FacilityEventsFields | Sequence[FacilityEventsFields] = None,
        query: str | None = None,
        search_properties: FacilityEventsTextFields | Sequence[FacilityEventsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_event_type_id: str | list[str] | None = None,
        facility_event_type_id_prefix: str | None = None,
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
        property: FacilityEventsFields | Sequence[FacilityEventsFields] | None = None,
        group_by: FacilityEventsFields | Sequence[FacilityEventsFields] | None = None,
        query: str | None = None,
        search_property: FacilityEventsTextFields | Sequence[FacilityEventsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_event_type_id: str | list[str] | None = None,
        facility_event_type_id_prefix: str | None = None,
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
            facility_event_type_id,
            facility_event_type_id_prefix,
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
            _FACILITYEVENTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FacilityEventsFields,
        interval: float,
        query: str | None = None,
        search_property: FacilityEventsTextFields | Sequence[FacilityEventsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_event_type_id: str | list[str] | None = None,
        facility_event_type_id_prefix: str | None = None,
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
            facility_event_type_id,
            facility_event_type_id_prefix,
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
            _FACILITYEVENTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_event_type_id: str | list[str] | None = None,
        facility_event_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilityEventsList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_event_type_id,
            facility_event_type_id_prefix,
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
    facility_event_type_id: str | list[str] | None = None,
    facility_event_type_id_prefix: str | None = None,
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
    if facility_event_type_id and isinstance(facility_event_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityEventTypeID"), value=facility_event_type_id))
    if facility_event_type_id and isinstance(facility_event_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityEventTypeID"), values=facility_event_type_id))
    if facility_event_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityEventTypeID"), value=facility_event_type_id_prefix)
        )
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
