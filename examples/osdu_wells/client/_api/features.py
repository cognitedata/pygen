from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Features,
    FeaturesApply,
    FeaturesList,
    FeaturesApplyList,
    FeaturesFields,
    FeaturesTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._features import _FEATURES_PROPERTIES_BY_FIELD


class FeaturesAPI(TypeAPI[Features, FeaturesApply, FeaturesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FeaturesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Features,
            class_apply_type=FeaturesApply,
            class_list=FeaturesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, feature: FeaturesApply | Sequence[FeaturesApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(feature, FeaturesApply):
            instances = feature.to_instances_apply(self._view_by_write_class)
        else:
            instances = FeaturesApplyList(feature).to_instances_apply(self._view_by_write_class)
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
    def retrieve(self, external_id: str) -> Features:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FeaturesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Features | FeaturesList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FeaturesList:
        filter_ = _create_filter(
            self._view_id,
            geometry,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _FEATURES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: FeaturesFields | Sequence[FeaturesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
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
        property: FeaturesFields | Sequence[FeaturesFields] | None = None,
        group_by: FeaturesFields | Sequence[FeaturesFields] = None,
        query: str | None = None,
        search_properties: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
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
        property: FeaturesFields | Sequence[FeaturesFields] | None = None,
        group_by: FeaturesFields | Sequence[FeaturesFields] | None = None,
        query: str | None = None,
        search_property: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            geometry,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _FEATURES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FeaturesFields,
        interval: float,
        query: str | None = None,
        search_property: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            geometry,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _FEATURES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FeaturesList:
        filter_ = _create_filter(
            self._view_id,
            geometry,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if geometry and isinstance(geometry, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("geometry"),
                value={"space": "IntegrationTestsImmutable", "externalId": geometry},
            )
        )
    if geometry and isinstance(geometry, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("geometry"), value={"space": geometry[0], "externalId": geometry[1]}
            )
        )
    if geometry and isinstance(geometry, list) and isinstance(geometry[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("geometry"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in geometry],
            )
        )
    if geometry and isinstance(geometry, list) and isinstance(geometry[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("geometry"),
                values=[{"space": item[0], "externalId": item[1]} for item in geometry],
            )
        )
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
