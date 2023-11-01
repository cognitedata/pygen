from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    LineageAssertions,
    LineageAssertionsApply,
    LineageAssertionsList,
    LineageAssertionsApplyList,
    LineageAssertionsFields,
    LineageAssertionsTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._lineage_assertions import _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD


class LineageAssertionsAPI(TypeAPI[LineageAssertions, LineageAssertionsApply, LineageAssertionsList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=LineageAssertions,
            class_apply_type=LineageAssertionsApply,
            class_list=LineageAssertionsList,
        )
        self._view_id = view_id

    def apply(
        self, lineage_assertion: LineageAssertionsApply | Sequence[LineageAssertionsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(lineage_assertion, LineageAssertionsApply):
            instances = lineage_assertion.to_instances_apply(self._view_id)
        else:
            instances = LineageAssertionsApplyList(lineage_assertion).to_instances_apply(self._view_id)
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
    def retrieve(self, external_id: str) -> LineageAssertions:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> LineageAssertionsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> LineageAssertions | LineageAssertionsList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LineageAssertionsList:
        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
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
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: LineageAssertionsFields | Sequence[LineageAssertionsFields] = None,
        query: str | None = None,
        search_properties: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
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
        property: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        group_by: LineageAssertionsFields | Sequence[LineageAssertionsFields] | None = None,
        query: str | None = None,
        search_property: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: LineageAssertionsFields,
        interval: float,
        query: str | None = None,
        search_property: LineageAssertionsTextFields | Sequence[LineageAssertionsTextFields] | None = None,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _LINEAGEASSERTIONS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        id: str | list[str] | None = None,
        id_prefix: str | None = None,
        lineage_relationship_type: str | list[str] | None = None,
        lineage_relationship_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LineageAssertionsList:
        filter_ = _create_filter(
            self._view_id,
            id,
            id_prefix,
            lineage_relationship_type,
            lineage_relationship_type_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    id: str | list[str] | None = None,
    id_prefix: str | None = None,
    lineage_relationship_type: str | list[str] | None = None,
    lineage_relationship_type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if id and isinstance(id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ID"), value=id))
    if id and isinstance(id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ID"), values=id))
    if id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ID"), value=id_prefix))
    if lineage_relationship_type and isinstance(lineage_relationship_type, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("LineageRelationshipType"), value=lineage_relationship_type)
        )
    if lineage_relationship_type and isinstance(lineage_relationship_type, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("LineageRelationshipType"), values=lineage_relationship_type)
        )
    if lineage_relationship_type_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("LineageRelationshipType"), value=lineage_relationship_type_prefix
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
