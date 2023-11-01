from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    WellboreCosts,
    WellboreCostsApply,
    WellboreCostsList,
    WellboreCostsApplyList,
    WellboreCostsFields,
    WellboreCostsTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._wellbore_costs import _WELLBORECOSTS_PROPERTIES_BY_FIELD


class WellboreCostsAPI(TypeAPI[WellboreCosts, WellboreCostsApply, WellboreCostsList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreCosts,
            class_apply_type=WellboreCostsApply,
            class_list=WellboreCostsList,
        )
        self._view_id = view_id

    def apply(
        self, wellbore_cost: WellboreCostsApply | Sequence[WellboreCostsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(wellbore_cost, WellboreCostsApply):
            instances = wellbore_cost.to_instances_apply(self._view_id)
        else:
            instances = WellboreCostsApplyList(wellbore_cost).to_instances_apply(self._view_id)
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
    def retrieve(self, external_id: str) -> WellboreCosts:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WellboreCostsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> WellboreCosts | WellboreCostsList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreCostsList:
        filter_ = _create_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _WELLBORECOSTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
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
        property: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        group_by: WellboreCostsFields | Sequence[WellboreCostsFields] = None,
        query: str | None = None,
        search_properties: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
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
        property: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        group_by: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        query: str | None = None,
        search_property: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLBORECOSTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellboreCostsFields,
        interval: float,
        query: str | None = None,
        search_property: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLBORECOSTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreCostsList:
        filter_ = _create_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    activity_type_id: str | list[str] | None = None,
    activity_type_id_prefix: str | None = None,
    min_cost: float | None = None,
    max_cost: float | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if activity_type_id and isinstance(activity_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ActivityTypeID"), value=activity_type_id))
    if activity_type_id and isinstance(activity_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ActivityTypeID"), values=activity_type_id))
    if activity_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ActivityTypeID"), value=activity_type_id_prefix))
    if min_cost or max_cost:
        filters.append(dm.filters.Range(view_id.as_property_ref("Cost"), gte=min_cost, lte=max_cost))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
