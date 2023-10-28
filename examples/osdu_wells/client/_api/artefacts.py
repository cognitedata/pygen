from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Artefacts,
    ArtefactsApply,
    ArtefactsList,
    ArtefactsApplyList,
    ArtefactsFields,
    ArtefactsTextFields,
)
from osdu_wells.client.data_classes._artefacts import _ARTEFACTS_PROPERTIES_BY_FIELD


class ArtefactsAPI(TypeAPI[Artefacts, ArtefactsApply, ArtefactsList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Artefacts,
            class_apply_type=ArtefactsApply,
            class_list=ArtefactsList,
        )
        self._view_id = view_id

    def apply(
        self, artefact: ArtefactsApply | Sequence[ArtefactsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(artefact, ArtefactsApply):
            instances = artefact.to_instances_apply()
        else:
            instances = ArtefactsApplyList(artefact).to_instances_apply()
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
    def retrieve(self, external_id: str) -> Artefacts:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ArtefactsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Artefacts | ArtefactsList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ArtefactsList:
        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _ARTEFACTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
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
        property: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        group_by: ArtefactsFields | Sequence[ArtefactsFields] = None,
        query: str | None = None,
        search_properties: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
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
        property: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        group_by: ArtefactsFields | Sequence[ArtefactsFields] | None = None,
        query: str | None = None,
        search_property: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ARTEFACTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ArtefactsFields,
        interval: float,
        query: str | None = None,
        search_property: ArtefactsTextFields | Sequence[ArtefactsTextFields] | None = None,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ARTEFACTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        resource_id: str | list[str] | None = None,
        resource_id_prefix: str | None = None,
        resource_kind: str | list[str] | None = None,
        resource_kind_prefix: str | None = None,
        role_id: str | list[str] | None = None,
        role_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ArtefactsList:
        filter_ = _create_filter(
            self._view_id,
            resource_id,
            resource_id_prefix,
            resource_kind,
            resource_kind_prefix,
            role_id,
            role_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    resource_id: str | list[str] | None = None,
    resource_id_prefix: str | None = None,
    resource_kind: str | list[str] | None = None,
    resource_kind_prefix: str | None = None,
    role_id: str | list[str] | None = None,
    role_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if resource_id and isinstance(resource_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ResourceID"), value=resource_id))
    if resource_id and isinstance(resource_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceID"), values=resource_id))
    if resource_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ResourceID"), value=resource_id_prefix))
    if resource_kind and isinstance(resource_kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ResourceKind"), value=resource_kind))
    if resource_kind and isinstance(resource_kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceKind"), values=resource_kind))
    if resource_kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ResourceKind"), value=resource_kind_prefix))
    if role_id and isinstance(role_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RoleID"), value=role_id))
    if role_id and isinstance(role_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RoleID"), values=role_id))
    if role_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RoleID"), value=role_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
