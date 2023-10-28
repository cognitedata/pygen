from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    VerticalMeasurement,
    VerticalMeasurementApply,
    VerticalMeasurementList,
    VerticalMeasurementApplyList,
    VerticalMeasurementFields,
    VerticalMeasurementTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._vertical_measurement import _VERTICALMEASUREMENT_PROPERTIES_BY_FIELD


class VerticalMeasurementAPI(TypeAPI[VerticalMeasurement, VerticalMeasurementApply, VerticalMeasurementList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=VerticalMeasurement,
            class_apply_type=VerticalMeasurementApply,
            class_list=VerticalMeasurementList,
        )
        self._view_id = view_id

    def apply(
        self, vertical_measurement: VerticalMeasurementApply | Sequence[VerticalMeasurementApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(vertical_measurement, VerticalMeasurementApply):
            instances = vertical_measurement.to_instances_apply()
        else:
            instances = VerticalMeasurementApplyList(vertical_measurement).to_instances_apply()
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
    def retrieve(self, external_id: str) -> VerticalMeasurement:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> VerticalMeasurementList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> VerticalMeasurement | VerticalMeasurementList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: VerticalMeasurementTextFields | Sequence[VerticalMeasurementTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_path_id: str | list[str] | None = None,
        vertical_measurement_path_id_prefix: str | None = None,
        vertical_measurement_source_id: str | list[str] | None = None,
        vertical_measurement_source_id_prefix: str | None = None,
        vertical_measurement_type_id: str | list[str] | None = None,
        vertical_measurement_type_id_prefix: str | None = None,
        vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
        vertical_measurement_unit_of_measure_id_prefix: str | None = None,
        vertical_reference_entity_id: str | list[str] | None = None,
        vertical_reference_entity_id_prefix: str | None = None,
        vertical_reference_id: str | list[str] | None = None,
        vertical_reference_id_prefix: str | None = None,
        wellbore_tvd_trajectory_id: str | list[str] | None = None,
        wellbore_tvd_trajectory_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> VerticalMeasurementList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_path_id,
            vertical_measurement_path_id_prefix,
            vertical_measurement_source_id,
            vertical_measurement_source_id_prefix,
            vertical_measurement_type_id,
            vertical_measurement_type_id_prefix,
            vertical_measurement_unit_of_measure_id,
            vertical_measurement_unit_of_measure_id_prefix,
            vertical_reference_entity_id,
            vertical_reference_entity_id_prefix,
            vertical_reference_id,
            vertical_reference_id_prefix,
            wellbore_tvd_trajectory_id,
            wellbore_tvd_trajectory_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _VERTICALMEASUREMENT_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: VerticalMeasurementFields | Sequence[VerticalMeasurementFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: VerticalMeasurementTextFields | Sequence[VerticalMeasurementTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_path_id: str | list[str] | None = None,
        vertical_measurement_path_id_prefix: str | None = None,
        vertical_measurement_source_id: str | list[str] | None = None,
        vertical_measurement_source_id_prefix: str | None = None,
        vertical_measurement_type_id: str | list[str] | None = None,
        vertical_measurement_type_id_prefix: str | None = None,
        vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
        vertical_measurement_unit_of_measure_id_prefix: str | None = None,
        vertical_reference_entity_id: str | list[str] | None = None,
        vertical_reference_entity_id_prefix: str | None = None,
        vertical_reference_id: str | list[str] | None = None,
        vertical_reference_id_prefix: str | None = None,
        wellbore_tvd_trajectory_id: str | list[str] | None = None,
        wellbore_tvd_trajectory_id_prefix: str | None = None,
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
        property: VerticalMeasurementFields | Sequence[VerticalMeasurementFields] | None = None,
        group_by: VerticalMeasurementFields | Sequence[VerticalMeasurementFields] = None,
        query: str | None = None,
        search_properties: VerticalMeasurementTextFields | Sequence[VerticalMeasurementTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_path_id: str | list[str] | None = None,
        vertical_measurement_path_id_prefix: str | None = None,
        vertical_measurement_source_id: str | list[str] | None = None,
        vertical_measurement_source_id_prefix: str | None = None,
        vertical_measurement_type_id: str | list[str] | None = None,
        vertical_measurement_type_id_prefix: str | None = None,
        vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
        vertical_measurement_unit_of_measure_id_prefix: str | None = None,
        vertical_reference_entity_id: str | list[str] | None = None,
        vertical_reference_entity_id_prefix: str | None = None,
        vertical_reference_id: str | list[str] | None = None,
        vertical_reference_id_prefix: str | None = None,
        wellbore_tvd_trajectory_id: str | list[str] | None = None,
        wellbore_tvd_trajectory_id_prefix: str | None = None,
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
        property: VerticalMeasurementFields | Sequence[VerticalMeasurementFields] | None = None,
        group_by: VerticalMeasurementFields | Sequence[VerticalMeasurementFields] | None = None,
        query: str | None = None,
        search_property: VerticalMeasurementTextFields | Sequence[VerticalMeasurementTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_path_id: str | list[str] | None = None,
        vertical_measurement_path_id_prefix: str | None = None,
        vertical_measurement_source_id: str | list[str] | None = None,
        vertical_measurement_source_id_prefix: str | None = None,
        vertical_measurement_type_id: str | list[str] | None = None,
        vertical_measurement_type_id_prefix: str | None = None,
        vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
        vertical_measurement_unit_of_measure_id_prefix: str | None = None,
        vertical_reference_entity_id: str | list[str] | None = None,
        vertical_reference_entity_id_prefix: str | None = None,
        vertical_reference_id: str | list[str] | None = None,
        vertical_reference_id_prefix: str | None = None,
        wellbore_tvd_trajectory_id: str | list[str] | None = None,
        wellbore_tvd_trajectory_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_path_id,
            vertical_measurement_path_id_prefix,
            vertical_measurement_source_id,
            vertical_measurement_source_id_prefix,
            vertical_measurement_type_id,
            vertical_measurement_type_id_prefix,
            vertical_measurement_unit_of_measure_id,
            vertical_measurement_unit_of_measure_id_prefix,
            vertical_reference_entity_id,
            vertical_reference_entity_id_prefix,
            vertical_reference_id,
            vertical_reference_id_prefix,
            wellbore_tvd_trajectory_id,
            wellbore_tvd_trajectory_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _VERTICALMEASUREMENT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: VerticalMeasurementFields,
        interval: float,
        query: str | None = None,
        search_property: VerticalMeasurementTextFields | Sequence[VerticalMeasurementTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_path_id: str | list[str] | None = None,
        vertical_measurement_path_id_prefix: str | None = None,
        vertical_measurement_source_id: str | list[str] | None = None,
        vertical_measurement_source_id_prefix: str | None = None,
        vertical_measurement_type_id: str | list[str] | None = None,
        vertical_measurement_type_id_prefix: str | None = None,
        vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
        vertical_measurement_unit_of_measure_id_prefix: str | None = None,
        vertical_reference_entity_id: str | list[str] | None = None,
        vertical_reference_entity_id_prefix: str | None = None,
        vertical_reference_id: str | list[str] | None = None,
        vertical_reference_id_prefix: str | None = None,
        wellbore_tvd_trajectory_id: str | list[str] | None = None,
        wellbore_tvd_trajectory_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_path_id,
            vertical_measurement_path_id_prefix,
            vertical_measurement_source_id,
            vertical_measurement_source_id_prefix,
            vertical_measurement_type_id,
            vertical_measurement_type_id_prefix,
            vertical_measurement_unit_of_measure_id,
            vertical_measurement_unit_of_measure_id_prefix,
            vertical_reference_entity_id,
            vertical_reference_entity_id_prefix,
            vertical_reference_id,
            vertical_reference_id_prefix,
            wellbore_tvd_trajectory_id,
            wellbore_tvd_trajectory_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _VERTICALMEASUREMENT_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_path_id: str | list[str] | None = None,
        vertical_measurement_path_id_prefix: str | None = None,
        vertical_measurement_source_id: str | list[str] | None = None,
        vertical_measurement_source_id_prefix: str | None = None,
        vertical_measurement_type_id: str | list[str] | None = None,
        vertical_measurement_type_id_prefix: str | None = None,
        vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
        vertical_measurement_unit_of_measure_id_prefix: str | None = None,
        vertical_reference_entity_id: str | list[str] | None = None,
        vertical_reference_entity_id_prefix: str | None = None,
        vertical_reference_id: str | list[str] | None = None,
        vertical_reference_id_prefix: str | None = None,
        wellbore_tvd_trajectory_id: str | list[str] | None = None,
        wellbore_tvd_trajectory_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> VerticalMeasurementList:
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_path_id,
            vertical_measurement_path_id_prefix,
            vertical_measurement_source_id,
            vertical_measurement_source_id_prefix,
            vertical_measurement_type_id,
            vertical_measurement_type_id_prefix,
            vertical_measurement_unit_of_measure_id,
            vertical_measurement_unit_of_measure_id_prefix,
            vertical_reference_entity_id,
            vertical_reference_entity_id_prefix,
            vertical_reference_id,
            vertical_reference_id_prefix,
            wellbore_tvd_trajectory_id,
            wellbore_tvd_trajectory_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    vertical_crsid: str | list[str] | None = None,
    vertical_crsid_prefix: str | None = None,
    min_vertical_measurement: float | None = None,
    max_vertical_measurement: float | None = None,
    vertical_measurement_description: str | list[str] | None = None,
    vertical_measurement_description_prefix: str | None = None,
    vertical_measurement_path_id: str | list[str] | None = None,
    vertical_measurement_path_id_prefix: str | None = None,
    vertical_measurement_source_id: str | list[str] | None = None,
    vertical_measurement_source_id_prefix: str | None = None,
    vertical_measurement_type_id: str | list[str] | None = None,
    vertical_measurement_type_id_prefix: str | None = None,
    vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
    vertical_measurement_unit_of_measure_id_prefix: str | None = None,
    vertical_reference_entity_id: str | list[str] | None = None,
    vertical_reference_entity_id_prefix: str | None = None,
    vertical_reference_id: str | list[str] | None = None,
    vertical_reference_id_prefix: str | None = None,
    wellbore_tvd_trajectory_id: str | list[str] | None = None,
    wellbore_tvd_trajectory_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
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
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if vertical_crsid and isinstance(vertical_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("VerticalCRSID"), value=vertical_crsid))
    if vertical_crsid and isinstance(vertical_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalCRSID"), values=vertical_crsid))
    if vertical_crsid_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("VerticalCRSID"), value=vertical_crsid_prefix))
    if min_vertical_measurement or max_vertical_measurement:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("VerticalMeasurement"),
                gte=min_vertical_measurement,
                lte=max_vertical_measurement,
            )
        )
    if vertical_measurement_description and isinstance(vertical_measurement_description, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurementDescription"), value=vertical_measurement_description
            )
        )
    if vertical_measurement_description and isinstance(vertical_measurement_description, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurementDescription"), values=vertical_measurement_description
            )
        )
    if vertical_measurement_description_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementDescription"), value=vertical_measurement_description_prefix
            )
        )
    if vertical_measurement_path_id and isinstance(vertical_measurement_path_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VerticalMeasurementPathID"), value=vertical_measurement_path_id)
        )
    if vertical_measurement_path_id and isinstance(vertical_measurement_path_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalMeasurementPathID"), values=vertical_measurement_path_id)
        )
    if vertical_measurement_path_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementPathID"), value=vertical_measurement_path_id_prefix
            )
        )
    if vertical_measurement_source_id and isinstance(vertical_measurement_source_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurementSourceID"), value=vertical_measurement_source_id
            )
        )
    if vertical_measurement_source_id and isinstance(vertical_measurement_source_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalMeasurementSourceID"), values=vertical_measurement_source_id)
        )
    if vertical_measurement_source_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementSourceID"), value=vertical_measurement_source_id_prefix
            )
        )
    if vertical_measurement_type_id and isinstance(vertical_measurement_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VerticalMeasurementTypeID"), value=vertical_measurement_type_id)
        )
    if vertical_measurement_type_id and isinstance(vertical_measurement_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalMeasurementTypeID"), values=vertical_measurement_type_id)
        )
    if vertical_measurement_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementTypeID"), value=vertical_measurement_type_id_prefix
            )
        )
    if vertical_measurement_unit_of_measure_id and isinstance(vertical_measurement_unit_of_measure_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurementUnitOfMeasureID"),
                value=vertical_measurement_unit_of_measure_id,
            )
        )
    if vertical_measurement_unit_of_measure_id and isinstance(vertical_measurement_unit_of_measure_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurementUnitOfMeasureID"),
                values=vertical_measurement_unit_of_measure_id,
            )
        )
    if vertical_measurement_unit_of_measure_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementUnitOfMeasureID"),
                value=vertical_measurement_unit_of_measure_id_prefix,
            )
        )
    if vertical_reference_entity_id and isinstance(vertical_reference_entity_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VerticalReferenceEntityID"), value=vertical_reference_entity_id)
        )
    if vertical_reference_entity_id and isinstance(vertical_reference_entity_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalReferenceEntityID"), values=vertical_reference_entity_id)
        )
    if vertical_reference_entity_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalReferenceEntityID"), value=vertical_reference_entity_id_prefix
            )
        )
    if vertical_reference_id and isinstance(vertical_reference_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("VerticalReferenceID"), value=vertical_reference_id))
    if vertical_reference_id and isinstance(vertical_reference_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalReferenceID"), values=vertical_reference_id))
    if vertical_reference_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("VerticalReferenceID"), value=vertical_reference_id_prefix)
        )
    if wellbore_tvd_trajectory_id and isinstance(wellbore_tvd_trajectory_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("WellboreTVDTrajectoryID"), value=wellbore_tvd_trajectory_id)
        )
    if wellbore_tvd_trajectory_id and isinstance(wellbore_tvd_trajectory_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("WellboreTVDTrajectoryID"), values=wellbore_tvd_trajectory_id)
        )
    if wellbore_tvd_trajectory_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("WellboreTVDTrajectoryID"), value=wellbore_tvd_trajectory_id_prefix
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
