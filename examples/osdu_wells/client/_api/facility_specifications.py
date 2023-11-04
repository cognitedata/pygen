from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    FacilitySpecifications,
    FacilitySpecificationsApply,
    FacilitySpecificationsList,
    FacilitySpecificationsApplyList,
    FacilitySpecificationsFields,
    FacilitySpecificationsTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._facility_specifications import _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD


class FacilitySpecificationsAPI(
    TypeAPI[FacilitySpecifications, FacilitySpecificationsApply, FacilitySpecificationsList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FacilitySpecificationsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FacilitySpecifications,
            class_apply_type=FacilitySpecificationsApply,
            class_list=FacilitySpecificationsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self,
        facility_specification: FacilitySpecificationsApply | Sequence[FacilitySpecificationsApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) facility specifications.

        Args:
            facility_specification: Facility specification or sequence of facility specifications to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            InstancesApplyResult: Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new facility_specification:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import FacilitySpecificationsApply
                >>> client = OSDUClient()
                >>> facility_specification = FacilitySpecificationsApply(external_id="my_facility_specification", ...)
                >>> result = client.facility_specifications.apply(facility_specification)

        """
        if isinstance(facility_specification, FacilitySpecificationsApply):
            instances = facility_specification.to_instances_apply(self._view_by_write_class)
        else:
            instances = FacilitySpecificationsApplyList(facility_specification).to_instances_apply(
                self._view_by_write_class
            )
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
    def retrieve(self, external_id: str) -> FacilitySpecifications:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FacilitySpecificationsList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> FacilitySpecifications | FacilitySpecificationsList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilitySpecificationsList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        group_by: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] = None,
        query: str | None = None,
        search_properties: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        group_by: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        query: str | None = None,
        search_property: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FacilitySpecificationsFields,
        interval: float,
        query: str | None = None,
        search_property: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilitySpecificationsList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    facility_specification_date_time: str | list[str] | None = None,
    facility_specification_date_time_prefix: str | None = None,
    facility_specification_indicator: bool | None = None,
    min_facility_specification_quantity: float | None = None,
    max_facility_specification_quantity: float | None = None,
    facility_specification_text: str | list[str] | None = None,
    facility_specification_text_prefix: str | None = None,
    parameter_type_id: str | list[str] | None = None,
    parameter_type_id_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    unit_of_measure_id: str | list[str] | None = None,
    unit_of_measure_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if effective_date_time and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if facility_specification_date_time and isinstance(facility_specification_date_time, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilitySpecificationDateTime"), value=facility_specification_date_time
            )
        )
    if facility_specification_date_time and isinstance(facility_specification_date_time, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("FacilitySpecificationDateTime"), values=facility_specification_date_time
            )
        )
    if facility_specification_date_time_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilitySpecificationDateTime"), value=facility_specification_date_time_prefix
            )
        )
    if facility_specification_indicator and isinstance(facility_specification_indicator, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilitySpecificationIndicator"), value=facility_specification_indicator
            )
        )
    if min_facility_specification_quantity or max_facility_specification_quantity:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("FacilitySpecificationQuantity"),
                gte=min_facility_specification_quantity,
                lte=max_facility_specification_quantity,
            )
        )
    if facility_specification_text and isinstance(facility_specification_text, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("FacilitySpecificationText"), value=facility_specification_text)
        )
    if facility_specification_text and isinstance(facility_specification_text, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("FacilitySpecificationText"), values=facility_specification_text)
        )
    if facility_specification_text_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilitySpecificationText"), value=facility_specification_text_prefix
            )
        )
    if parameter_type_id and isinstance(parameter_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ParameterTypeID"), value=parameter_type_id))
    if parameter_type_id and isinstance(parameter_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ParameterTypeID"), values=parameter_type_id))
    if parameter_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ParameterTypeID"), value=parameter_type_id_prefix))
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if unit_of_measure_id and isinstance(unit_of_measure_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("UnitOfMeasureID"), value=unit_of_measure_id))
    if unit_of_measure_id and isinstance(unit_of_measure_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("UnitOfMeasureID"), values=unit_of_measure_id))
    if unit_of_measure_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("UnitOfMeasureID"), value=unit_of_measure_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
