from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    NameAliases,
    NameAliasesApply,
    NameAliasesList,
    NameAliasesApplyList,
    NameAliasesFields,
    NameAliasesTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._name_aliases import _NAMEALIASES_PROPERTIES_BY_FIELD


class NameAliasesAPI(TypeAPI[NameAliases, NameAliasesApply, NameAliasesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[NameAliasesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=NameAliases,
            class_apply_type=NameAliasesApply,
            class_list=NameAliasesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, name_alias: NameAliasesApply | Sequence[NameAliasesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(name_alias, NameAliasesApply):
            instances = name_alias.to_instances_apply(self._view_by_write_class)
        else:
            instances = NameAliasesApplyList(name_alias).to_instances_apply(self._view_by_write_class)
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
    def retrieve(self, external_id: str) -> NameAliases:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> NameAliasesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> NameAliases | NameAliasesList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
        alias_name: str | list[str] | None = None,
        alias_name_prefix: str | None = None,
        alias_name_type_id: str | list[str] | None = None,
        alias_name_type_id_prefix: str | None = None,
        definition_organisation_id: str | list[str] | None = None,
        definition_organisation_id_prefix: str | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NameAliasesList:
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _NAMEALIASES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
        alias_name: str | list[str] | None = None,
        alias_name_prefix: str | None = None,
        alias_name_type_id: str | list[str] | None = None,
        alias_name_type_id_prefix: str | None = None,
        definition_organisation_id: str | list[str] | None = None,
        definition_organisation_id_prefix: str | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
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
        property: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        group_by: NameAliasesFields | Sequence[NameAliasesFields] = None,
        query: str | None = None,
        search_properties: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
        alias_name: str | list[str] | None = None,
        alias_name_prefix: str | None = None,
        alias_name_type_id: str | list[str] | None = None,
        alias_name_type_id_prefix: str | None = None,
        definition_organisation_id: str | list[str] | None = None,
        definition_organisation_id_prefix: str | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
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
        property: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        group_by: NameAliasesFields | Sequence[NameAliasesFields] | None = None,
        query: str | None = None,
        search_property: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
        alias_name: str | list[str] | None = None,
        alias_name_prefix: str | None = None,
        alias_name_type_id: str | list[str] | None = None,
        alias_name_type_id_prefix: str | None = None,
        definition_organisation_id: str | list[str] | None = None,
        definition_organisation_id_prefix: str | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _NAMEALIASES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: NameAliasesFields,
        interval: float,
        query: str | None = None,
        search_property: NameAliasesTextFields | Sequence[NameAliasesTextFields] | None = None,
        alias_name: str | list[str] | None = None,
        alias_name_prefix: str | None = None,
        alias_name_type_id: str | list[str] | None = None,
        alias_name_type_id_prefix: str | None = None,
        definition_organisation_id: str | list[str] | None = None,
        definition_organisation_id_prefix: str | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _NAMEALIASES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        alias_name: str | list[str] | None = None,
        alias_name_prefix: str | None = None,
        alias_name_type_id: str | list[str] | None = None,
        alias_name_type_id_prefix: str | None = None,
        definition_organisation_id: str | list[str] | None = None,
        definition_organisation_id_prefix: str | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NameAliasesList:
        filter_ = _create_filter(
            self._view_id,
            alias_name,
            alias_name_prefix,
            alias_name_type_id,
            alias_name_type_id_prefix,
            definition_organisation_id,
            definition_organisation_id_prefix,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    alias_name: str | list[str] | None = None,
    alias_name_prefix: str | None = None,
    alias_name_type_id: str | list[str] | None = None,
    alias_name_type_id_prefix: str | None = None,
    definition_organisation_id: str | list[str] | None = None,
    definition_organisation_id_prefix: str | None = None,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if alias_name and isinstance(alias_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AliasName"), value=alias_name))
    if alias_name and isinstance(alias_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AliasName"), values=alias_name))
    if alias_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AliasName"), value=alias_name_prefix))
    if alias_name_type_id and isinstance(alias_name_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AliasNameTypeID"), value=alias_name_type_id))
    if alias_name_type_id and isinstance(alias_name_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AliasNameTypeID"), values=alias_name_type_id))
    if alias_name_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AliasNameTypeID"), value=alias_name_type_id_prefix))
    if definition_organisation_id and isinstance(definition_organisation_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DefinitionOrganisationID"), value=definition_organisation_id)
        )
    if definition_organisation_id and isinstance(definition_organisation_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("DefinitionOrganisationID"), values=definition_organisation_id)
        )
    if definition_organisation_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DefinitionOrganisationID"), value=definition_organisation_id_prefix
            )
        )
    if effective_date_time and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
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
