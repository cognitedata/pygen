from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets.client.data_classes import (
    ValueTransformation,
    ValueTransformationApply,
    ValueTransformationList,
    ValueTransformationApplyList,
    ValueTransformationFields,
    ValueTransformationTextFields,
    DomainModelApply,
)
from markets.client.data_classes._value_transformation import _VALUETRANSFORMATION_PROPERTIES_BY_FIELD


class ValueTransformationAPI(TypeAPI[ValueTransformation, ValueTransformationApply, ValueTransformationList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ValueTransformationApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ValueTransformation,
            class_apply_type=ValueTransformationApply,
            class_list=ValueTransformationList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, value_transformation: ValueTransformationApply | Sequence[ValueTransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(value_transformation, ValueTransformationApply):
            instances = value_transformation.to_instances_apply(self._view_by_write_class)
        else:
            instances = ValueTransformationApplyList(value_transformation).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="market") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ValueTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ValueTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ValueTransformation | ValueTransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ValueTransformationList:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _VALUETRANSFORMATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
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
        property: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        group_by: ValueTransformationFields | Sequence[ValueTransformationFields] = None,
        query: str | None = None,
        search_properties: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
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
        property: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        group_by: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        query: str | None = None,
        search_property: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _VALUETRANSFORMATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ValueTransformationFields,
        interval: float,
        query: str | None = None,
        search_property: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _VALUETRANSFORMATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ValueTransformationList:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
