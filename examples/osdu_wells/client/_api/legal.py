from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import Legal, LegalApply, LegalList, LegalApplyList, LegalFields, LegalTextFields
from osdu_wells.client.data_classes._legal import _LEGAL_PROPERTIES_BY_FIELD


class LegalAPI(TypeAPI[Legal, LegalApply, LegalList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Legal,
            class_apply_type=LegalApply,
            class_list=LegalList,
        )
        self._view_id = view_id

    def apply(self, legal: LegalApply | Sequence[LegalApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(legal, LegalApply):
            instances = legal.to_instances_apply()
        else:
            instances = LegalApplyList(legal).to_instances_apply()
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
    def retrieve(self, external_id: str) -> Legal:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> LegalList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Legal | LegalList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LegalList:
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _LEGAL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
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
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: LegalFields | Sequence[LegalFields] = None,
        query: str | None = None,
        search_properties: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
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
        property: LegalFields | Sequence[LegalFields] | None = None,
        group_by: LegalFields | Sequence[LegalFields] | None = None,
        query: str | None = None,
        search_property: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _LEGAL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: LegalFields,
        interval: float,
        query: str | None = None,
        search_property: LegalTextFields | Sequence[LegalTextFields] | None = None,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _LEGAL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        status: str | list[str] | None = None,
        status_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> LegalList:
        filter_ = _create_filter(
            self._view_id,
            status,
            status_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    status: str | list[str] | None = None,
    status_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if status and isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
    if status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("status"), value=status_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
