from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Meta,
    MetaApply,
    MetaList,
    MetaApplyList,
    MetaFields,
    MetaTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._meta import _META_PROPERTIES_BY_FIELD


class MetaAPI(TypeAPI[Meta, MetaApply, MetaList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MetaApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Meta,
            class_apply_type=MetaApply,
            class_list=MetaList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, meta: MetaApply | Sequence[MetaApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(meta, MetaApply):
            instances = meta.to_instances_apply(self._view_by_write_class)
        else:
            instances = MetaApplyList(meta).to_instances_apply(self._view_by_write_class)
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
    def retrieve(self, external_id: str) -> Meta:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MetaList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Meta | MetaList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MetaList:
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _META_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: MetaFields | Sequence[MetaFields] = None,
        query: str | None = None,
        search_properties: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: MetaFields | Sequence[MetaFields] | None = None,
        group_by: MetaFields | Sequence[MetaFields] | None = None,
        query: str | None = None,
        search_property: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _META_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MetaFields,
        interval: float,
        query: str | None = None,
        search_property: MetaTextFields | Sequence[MetaTextFields] | None = None,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _META_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        kind: str | list[str] | None = None,
        kind_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        persistable_reference: str | list[str] | None = None,
        persistable_reference_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MetaList:
        filter_ = _create_filter(
            self._view_id,
            kind,
            kind_prefix,
            name,
            name_prefix,
            persistable_reference,
            persistable_reference_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    kind: str | list[str] | None = None,
    kind_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    persistable_reference: str | list[str] | None = None,
    persistable_reference_prefix: str | None = None,
    unit_of_measure_id: str | list[str] | None = None,
    unit_of_measure_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if kind and isinstance(kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("kind"), value=kind))
    if kind and isinstance(kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("kind"), values=kind))
    if kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("kind"), value=kind_prefix))
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if persistable_reference and isinstance(persistable_reference, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("persistableReference"), value=persistable_reference))
    if persistable_reference and isinstance(persistable_reference, list):
        filters.append(dm.filters.In(view_id.as_property_ref("persistableReference"), values=persistable_reference))
    if persistable_reference_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("persistableReference"), value=persistable_reference_prefix)
        )
    if unit_of_measure_id and isinstance(unit_of_measure_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unitOfMeasureID"), value=unit_of_measure_id))
    if unit_of_measure_id and isinstance(unit_of_measure_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unitOfMeasureID"), values=unit_of_measure_id))
    if unit_of_measure_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unitOfMeasureID"), value=unit_of_measure_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
