from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets_pydantic_v1.client.data_classes import (
    CogProcess,
    CogProcessApply,
    CogProcessList,
    CogProcessApplyList,
    CogProcessFields,
    CogProcessTextFields,
    DomainModelApply,
)
from markets_pydantic_v1.client.data_classes._cog_process import _COGPROCESS_PROPERTIES_BY_FIELD


class CogProcessAPI(TypeAPI[CogProcess, CogProcessApply, CogProcessList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CogProcessApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CogProcess,
            class_apply_type=CogProcessApply,
            class_list=CogProcessList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, cog_proces: CogProcessApply | Sequence[CogProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(cog_proces, CogProcessApply):
            instances = cog_proces.to_instances_apply(self._view_by_write_class)
        else:
            instances = CogProcessApplyList(cog_proces).to_instances_apply(self._view_by_write_class)
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
    def retrieve(self, external_id: str) -> CogProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CogProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CogProcess | CogProcessList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogProcessList:
        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _COGPROCESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: CogProcessFields | Sequence[CogProcessFields] = None,
        query: str | None = None,
        search_properties: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: CogProcessFields | Sequence[CogProcessFields] | None = None,
        group_by: CogProcessFields | Sequence[CogProcessFields] | None = None,
        query: str | None = None,
        search_property: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _COGPROCESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CogProcessFields,
        interval: float,
        query: str | None = None,
        search_property: CogProcessTextFields | Sequence[CogProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _COGPROCESS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CogProcessList:
        filter_ = _create_filter(
            self._view_id,
            bid,
            date_transformations,
            name,
            name_prefix,
            transformation,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    date_transformations: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    transformation: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if bid and isinstance(bid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": "market", "externalId": bid}))
    if bid and isinstance(bid, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": bid[0], "externalId": bid[1]}))
    if bid and isinstance(bid, list) and isinstance(bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": "market", "externalId": item} for item in bid]
            )
        )
    if bid and isinstance(bid, list) and isinstance(bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": item[0], "externalId": item[1]} for item in bid]
            )
        )
    if date_transformations and isinstance(date_transformations, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("dateTransformations"),
                value={"space": "market", "externalId": date_transformations},
            )
        )
    if date_transformations and isinstance(date_transformations, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("dateTransformations"),
                value={"space": date_transformations[0], "externalId": date_transformations[1]},
            )
        )
    if date_transformations and isinstance(date_transformations, list) and isinstance(date_transformations[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("dateTransformations"),
                values=[{"space": "market", "externalId": item} for item in date_transformations],
            )
        )
    if date_transformations and isinstance(date_transformations, list) and isinstance(date_transformations[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("dateTransformations"),
                values=[{"space": item[0], "externalId": item[1]} for item in date_transformations],
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if transformation and isinstance(transformation, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("transformation"), value={"space": "market", "externalId": transformation}
            )
        )
    if transformation and isinstance(transformation, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("transformation"),
                value={"space": transformation[0], "externalId": transformation[1]},
            )
        )
    if transformation and isinstance(transformation, list) and isinstance(transformation[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("transformation"),
                values=[{"space": "market", "externalId": item} for item in transformation],
            )
        )
    if transformation and isinstance(transformation, list) and isinstance(transformation[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("transformation"),
                values=[{"space": item[0], "externalId": item[1]} for item in transformation],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
