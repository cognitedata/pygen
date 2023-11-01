from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    GeoContexts,
    GeoContextsApply,
    GeoContextsList,
    GeoContextsApplyList,
    GeoContextsFields,
    GeoContextsTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._geo_contexts import _GEOCONTEXTS_PROPERTIES_BY_FIELD


class GeoContextsAPI(TypeAPI[GeoContexts, GeoContextsApply, GeoContextsList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=GeoContexts,
            class_apply_type=GeoContextsApply,
            class_list=GeoContextsList,
        )
        self._view_id = view_id

    def apply(
        self, geo_context: GeoContextsApply | Sequence[GeoContextsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(geo_context, GeoContextsApply):
            instances = geo_context.to_instances_apply(self._view_id)
        else:
            instances = GeoContextsApplyList(geo_context).to_instances_apply(self._view_id)
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
    def retrieve(self, external_id: str) -> GeoContexts:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> GeoContextsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> GeoContexts | GeoContextsList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeoContextsList:
        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _GEOCONTEXTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
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
        property: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        group_by: GeoContextsFields | Sequence[GeoContextsFields] = None,
        query: str | None = None,
        search_properties: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
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
        property: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        group_by: GeoContextsFields | Sequence[GeoContextsFields] | None = None,
        query: str | None = None,
        search_property: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _GEOCONTEXTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: GeoContextsFields,
        interval: float,
        query: str | None = None,
        search_property: GeoContextsTextFields | Sequence[GeoContextsTextFields] | None = None,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _GEOCONTEXTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        basin_id: str | list[str] | None = None,
        basin_id_prefix: str | None = None,
        field_id: str | list[str] | None = None,
        field_id_prefix: str | None = None,
        geo_political_entity_id: str | list[str] | None = None,
        geo_political_entity_id_prefix: str | None = None,
        geo_type_id: str | list[str] | None = None,
        geo_type_id_prefix: str | None = None,
        play_id: str | list[str] | None = None,
        play_id_prefix: str | None = None,
        prospect_id: str | list[str] | None = None,
        prospect_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeoContextsList:
        filter_ = _create_filter(
            self._view_id,
            basin_id,
            basin_id_prefix,
            field_id,
            field_id_prefix,
            geo_political_entity_id,
            geo_political_entity_id_prefix,
            geo_type_id,
            geo_type_id_prefix,
            play_id,
            play_id_prefix,
            prospect_id,
            prospect_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    basin_id: str | list[str] | None = None,
    basin_id_prefix: str | None = None,
    field_id: str | list[str] | None = None,
    field_id_prefix: str | None = None,
    geo_political_entity_id: str | list[str] | None = None,
    geo_political_entity_id_prefix: str | None = None,
    geo_type_id: str | list[str] | None = None,
    geo_type_id_prefix: str | None = None,
    play_id: str | list[str] | None = None,
    play_id_prefix: str | None = None,
    prospect_id: str | list[str] | None = None,
    prospect_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if basin_id and isinstance(basin_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("BasinID"), value=basin_id))
    if basin_id and isinstance(basin_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("BasinID"), values=basin_id))
    if basin_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("BasinID"), value=basin_id_prefix))
    if field_id and isinstance(field_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FieldID"), value=field_id))
    if field_id and isinstance(field_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FieldID"), values=field_id))
    if field_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FieldID"), value=field_id_prefix))
    if geo_political_entity_id and isinstance(geo_political_entity_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("GeoPoliticalEntityID"), value=geo_political_entity_id)
        )
    if geo_political_entity_id and isinstance(geo_political_entity_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeoPoliticalEntityID"), values=geo_political_entity_id))
    if geo_political_entity_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("GeoPoliticalEntityID"), value=geo_political_entity_id_prefix)
        )
    if geo_type_id and isinstance(geo_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("GeoTypeID"), value=geo_type_id))
    if geo_type_id and isinstance(geo_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeoTypeID"), values=geo_type_id))
    if geo_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("GeoTypeID"), value=geo_type_id_prefix))
    if play_id and isinstance(play_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("PlayID"), value=play_id))
    if play_id and isinstance(play_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("PlayID"), values=play_id))
    if play_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("PlayID"), value=play_id_prefix))
    if prospect_id and isinstance(prospect_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ProspectID"), value=prospect_id))
    if prospect_id and isinstance(prospect_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ProspectID"), values=prospect_id))
    if prospect_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ProspectID"), value=prospect_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
