from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    VerticalMeasurement,
    VerticalMeasurementApply,
    VerticalMeasurementFields,
    VerticalMeasurementList,
    VerticalMeasurementApplyList,
    VerticalMeasurementTextFields,
)
from osdu_wells.client.data_classes._vertical_measurement import (
    _VERTICALMEASUREMENT_PROPERTIES_BY_FIELD,
    _create_vertical_measurement_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .vertical_measurement_query import VerticalMeasurementQueryAPI


class VerticalMeasurementAPI(NodeAPI[VerticalMeasurement, VerticalMeasurementApply, VerticalMeasurementList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[VerticalMeasurementApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=VerticalMeasurement,
            class_apply_type=VerticalMeasurementApply,
            class_list=VerticalMeasurementList,
            class_apply_list=VerticalMeasurementApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> VerticalMeasurementQueryAPI[VerticalMeasurementList]:
        """Query starting at vertical measurements.

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_path_id: The vertical measurement path id to filter on.
            vertical_measurement_path_id_prefix: The prefix of the vertical measurement path id to filter on.
            vertical_measurement_source_id: The vertical measurement source id to filter on.
            vertical_measurement_source_id_prefix: The prefix of the vertical measurement source id to filter on.
            vertical_measurement_type_id: The vertical measurement type id to filter on.
            vertical_measurement_type_id_prefix: The prefix of the vertical measurement type id to filter on.
            vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id to filter on.
            vertical_measurement_unit_of_measure_id_prefix: The prefix of the vertical measurement unit of measure id to filter on.
            vertical_reference_entity_id: The vertical reference entity id to filter on.
            vertical_reference_entity_id_prefix: The prefix of the vertical reference entity id to filter on.
            vertical_reference_id: The vertical reference id to filter on.
            vertical_reference_id_prefix: The prefix of the vertical reference id to filter on.
            wellbore_tvd_trajectory_id: The wellbore tvd trajectory id to filter on.
            wellbore_tvd_trajectory_id_prefix: The prefix of the wellbore tvd trajectory id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of vertical measurements to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for vertical measurements.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_vertical_measurement_filter(
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
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(VerticalMeasurementList)
        return VerticalMeasurementQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, vertical_measurement: VerticalMeasurementApply | Sequence[VerticalMeasurementApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) vertical measurements.

        Args:
            vertical_measurement: Vertical measurement or sequence of vertical measurements to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new vertical_measurement:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import VerticalMeasurementApply
                >>> client = OSDUClient()
                >>> vertical_measurement = VerticalMeasurementApply(external_id="my_vertical_measurement", ...)
                >>> result = client.vertical_measurement.apply(vertical_measurement)

        """
        return self._apply(vertical_measurement, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more vertical measurement.

        Args:
            external_id: External id of the vertical measurement to delete.
            space: The space where all the vertical measurement are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete vertical_measurement by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.vertical_measurement.delete("my_vertical_measurement")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> VerticalMeasurement | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> VerticalMeasurementList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> VerticalMeasurement | VerticalMeasurementList | None:
        """Retrieve one or more vertical measurements by id(s).

        Args:
            external_id: External id or list of external ids of the vertical measurements.
            space: The space where all the vertical measurements are located.

        Returns:
            The requested vertical measurements.

        Examples:

            Retrieve vertical_measurement by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> vertical_measurement = client.vertical_measurement.retrieve("my_vertical_measurement")

        """
        return self._retrieve(external_id, space)

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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> VerticalMeasurementList:
        """Search vertical measurements

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_path_id: The vertical measurement path id to filter on.
            vertical_measurement_path_id_prefix: The prefix of the vertical measurement path id to filter on.
            vertical_measurement_source_id: The vertical measurement source id to filter on.
            vertical_measurement_source_id_prefix: The prefix of the vertical measurement source id to filter on.
            vertical_measurement_type_id: The vertical measurement type id to filter on.
            vertical_measurement_type_id_prefix: The prefix of the vertical measurement type id to filter on.
            vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id to filter on.
            vertical_measurement_unit_of_measure_id_prefix: The prefix of the vertical measurement unit of measure id to filter on.
            vertical_reference_entity_id: The vertical reference entity id to filter on.
            vertical_reference_entity_id_prefix: The prefix of the vertical reference entity id to filter on.
            vertical_reference_id: The vertical reference id to filter on.
            vertical_reference_id_prefix: The prefix of the vertical reference id to filter on.
            wellbore_tvd_trajectory_id: The wellbore tvd trajectory id to filter on.
            wellbore_tvd_trajectory_id_prefix: The prefix of the wellbore tvd trajectory id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of vertical measurements to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results vertical measurements matching the query.

        Examples:

           Search for 'my_vertical_measurement' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> vertical_measurements = client.vertical_measurement.search('my_vertical_measurement')

        """
        filter_ = _create_vertical_measurement_filter(
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
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across vertical measurements

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_path_id: The vertical measurement path id to filter on.
            vertical_measurement_path_id_prefix: The prefix of the vertical measurement path id to filter on.
            vertical_measurement_source_id: The vertical measurement source id to filter on.
            vertical_measurement_source_id_prefix: The prefix of the vertical measurement source id to filter on.
            vertical_measurement_type_id: The vertical measurement type id to filter on.
            vertical_measurement_type_id_prefix: The prefix of the vertical measurement type id to filter on.
            vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id to filter on.
            vertical_measurement_unit_of_measure_id_prefix: The prefix of the vertical measurement unit of measure id to filter on.
            vertical_reference_entity_id: The vertical reference entity id to filter on.
            vertical_reference_entity_id_prefix: The prefix of the vertical reference entity id to filter on.
            vertical_reference_id: The vertical reference id to filter on.
            vertical_reference_id_prefix: The prefix of the vertical reference id to filter on.
            wellbore_tvd_trajectory_id: The wellbore tvd trajectory id to filter on.
            wellbore_tvd_trajectory_id_prefix: The prefix of the wellbore tvd trajectory id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of vertical measurements to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count vertical measurements in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.vertical_measurement.aggregate("count", space="my_space")

        """

        filter_ = _create_vertical_measurement_filter(
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
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for vertical measurements

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_path_id: The vertical measurement path id to filter on.
            vertical_measurement_path_id_prefix: The prefix of the vertical measurement path id to filter on.
            vertical_measurement_source_id: The vertical measurement source id to filter on.
            vertical_measurement_source_id_prefix: The prefix of the vertical measurement source id to filter on.
            vertical_measurement_type_id: The vertical measurement type id to filter on.
            vertical_measurement_type_id_prefix: The prefix of the vertical measurement type id to filter on.
            vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id to filter on.
            vertical_measurement_unit_of_measure_id_prefix: The prefix of the vertical measurement unit of measure id to filter on.
            vertical_reference_entity_id: The vertical reference entity id to filter on.
            vertical_reference_entity_id_prefix: The prefix of the vertical reference entity id to filter on.
            vertical_reference_id: The vertical reference id to filter on.
            vertical_reference_id_prefix: The prefix of the vertical reference id to filter on.
            wellbore_tvd_trajectory_id: The wellbore tvd trajectory id to filter on.
            wellbore_tvd_trajectory_id_prefix: The prefix of the wellbore tvd trajectory id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of vertical measurements to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_vertical_measurement_filter(
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
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> VerticalMeasurementList:
        """List/filter vertical measurements

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_path_id: The vertical measurement path id to filter on.
            vertical_measurement_path_id_prefix: The prefix of the vertical measurement path id to filter on.
            vertical_measurement_source_id: The vertical measurement source id to filter on.
            vertical_measurement_source_id_prefix: The prefix of the vertical measurement source id to filter on.
            vertical_measurement_type_id: The vertical measurement type id to filter on.
            vertical_measurement_type_id_prefix: The prefix of the vertical measurement type id to filter on.
            vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id to filter on.
            vertical_measurement_unit_of_measure_id_prefix: The prefix of the vertical measurement unit of measure id to filter on.
            vertical_reference_entity_id: The vertical reference entity id to filter on.
            vertical_reference_entity_id_prefix: The prefix of the vertical reference entity id to filter on.
            vertical_reference_id: The vertical reference id to filter on.
            vertical_reference_id_prefix: The prefix of the vertical reference id to filter on.
            wellbore_tvd_trajectory_id: The wellbore tvd trajectory id to filter on.
            wellbore_tvd_trajectory_id_prefix: The prefix of the wellbore tvd trajectory id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of vertical measurements to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested vertical measurements

        Examples:

            List vertical measurements and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> vertical_measurements = client.vertical_measurement.list(limit=5)

        """
        filter_ = _create_vertical_measurement_filter(
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
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
