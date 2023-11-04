from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    VerticalMeasurements,
    VerticalMeasurementsApply,
    VerticalMeasurementsList,
    VerticalMeasurementsApplyList,
    VerticalMeasurementsFields,
    VerticalMeasurementsTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._vertical_measurements import _VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD


class VerticalMeasurementsAPI(TypeAPI[VerticalMeasurements, VerticalMeasurementsApply, VerticalMeasurementsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[VerticalMeasurementsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=VerticalMeasurements,
            class_apply_type=VerticalMeasurementsApply,
            class_list=VerticalMeasurementsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self,
        vertical_measurement: VerticalMeasurementsApply | Sequence[VerticalMeasurementsApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) vertical measurements.

        Args:
            vertical_measurement: Vertical measurement or sequence of vertical measurements to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new vertical_measurement:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import VerticalMeasurementsApply
                >>> client = OSDUClient()
                >>> vertical_measurement = VerticalMeasurementsApply(external_id="my_vertical_measurement", ...)
                >>> result = client.vertical_measurements.apply(vertical_measurement)

        """
        if isinstance(vertical_measurement, VerticalMeasurementsApply):
            instances = vertical_measurement.to_instances_apply(self._view_by_write_class)
        else:
            instances = VerticalMeasurementsApplyList(vertical_measurement).to_instances_apply(
                self._view_by_write_class
            )
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
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
                >>> client.vertical_measurements.delete("my_vertical_measurement")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> VerticalMeasurements:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> VerticalMeasurementsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> VerticalMeasurements | VerticalMeasurementsList:
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
                >>> vertical_measurement = client.vertical_measurements.retrieve("my_vertical_measurement")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: VerticalMeasurementsTextFields | Sequence[VerticalMeasurementsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        rig_id: str | list[str] | None = None,
        rig_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_id: str | list[str] | None = None,
        vertical_measurement_id_prefix: str | None = None,
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
    ) -> VerticalMeasurementsList:
        """Search vertical measurements

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            rig_id: The rig id to filter on.
            rig_id_prefix: The prefix of the rig id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_id: The vertical measurement id to filter on.
            vertical_measurement_id_prefix: The prefix of the vertical measurement id to filter on.
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
                >>> vertical_measurements = client.vertical_measurements.search('my_vertical_measurement')

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            rig_id,
            rig_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_id,
            vertical_measurement_id_prefix,
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
        return self._search(self._view_id, query, _VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: VerticalMeasurementsFields | Sequence[VerticalMeasurementsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: VerticalMeasurementsTextFields | Sequence[VerticalMeasurementsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        rig_id: str | list[str] | None = None,
        rig_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_id: str | list[str] | None = None,
        vertical_measurement_id_prefix: str | None = None,
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
        property: VerticalMeasurementsFields | Sequence[VerticalMeasurementsFields] | None = None,
        group_by: VerticalMeasurementsFields | Sequence[VerticalMeasurementsFields] = None,
        query: str | None = None,
        search_properties: VerticalMeasurementsTextFields | Sequence[VerticalMeasurementsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        rig_id: str | list[str] | None = None,
        rig_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_id: str | list[str] | None = None,
        vertical_measurement_id_prefix: str | None = None,
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
        property: VerticalMeasurementsFields | Sequence[VerticalMeasurementsFields] | None = None,
        group_by: VerticalMeasurementsFields | Sequence[VerticalMeasurementsFields] | None = None,
        query: str | None = None,
        search_property: VerticalMeasurementsTextFields | Sequence[VerticalMeasurementsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        rig_id: str | list[str] | None = None,
        rig_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_id: str | list[str] | None = None,
        vertical_measurement_id_prefix: str | None = None,
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
            rig_id: The rig id to filter on.
            rig_id_prefix: The prefix of the rig id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_id: The vertical measurement id to filter on.
            vertical_measurement_id_prefix: The prefix of the vertical measurement id to filter on.
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
                >>> result = client.vertical_measurements.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            rig_id,
            rig_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_id,
            vertical_measurement_id_prefix,
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
            _VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: VerticalMeasurementsFields,
        interval: float,
        query: str | None = None,
        search_property: VerticalMeasurementsTextFields | Sequence[VerticalMeasurementsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        rig_id: str | list[str] | None = None,
        rig_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_id: str | list[str] | None = None,
        vertical_measurement_id_prefix: str | None = None,
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
            rig_id: The rig id to filter on.
            rig_id_prefix: The prefix of the rig id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_id: The vertical measurement id to filter on.
            vertical_measurement_id_prefix: The prefix of the vertical measurement id to filter on.
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
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            rig_id,
            rig_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_id,
            vertical_measurement_id_prefix,
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
            _VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        rig_id: str | list[str] | None = None,
        rig_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        vertical_crsid: str | list[str] | None = None,
        vertical_crsid_prefix: str | None = None,
        min_vertical_measurement: float | None = None,
        max_vertical_measurement: float | None = None,
        vertical_measurement_description: str | list[str] | None = None,
        vertical_measurement_description_prefix: str | None = None,
        vertical_measurement_id: str | list[str] | None = None,
        vertical_measurement_id_prefix: str | None = None,
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
    ) -> VerticalMeasurementsList:
        """List/filter vertical measurements

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            rig_id: The rig id to filter on.
            rig_id_prefix: The prefix of the rig id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            vertical_crsid: The vertical crsid to filter on.
            vertical_crsid_prefix: The prefix of the vertical crsid to filter on.
            min_vertical_measurement: The minimum value of the vertical measurement to filter on.
            max_vertical_measurement: The maximum value of the vertical measurement to filter on.
            vertical_measurement_description: The vertical measurement description to filter on.
            vertical_measurement_description_prefix: The prefix of the vertical measurement description to filter on.
            vertical_measurement_id: The vertical measurement id to filter on.
            vertical_measurement_id_prefix: The prefix of the vertical measurement id to filter on.
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
                >>> vertical_measurements = client.vertical_measurements.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            rig_id,
            rig_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            vertical_crsid,
            vertical_crsid_prefix,
            min_vertical_measurement,
            max_vertical_measurement,
            vertical_measurement_description,
            vertical_measurement_description_prefix,
            vertical_measurement_id,
            vertical_measurement_id_prefix,
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


def _create_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    rig_id: str | list[str] | None = None,
    rig_id_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    vertical_crsid: str | list[str] | None = None,
    vertical_crsid_prefix: str | None = None,
    min_vertical_measurement: float | None = None,
    max_vertical_measurement: float | None = None,
    vertical_measurement_description: str | list[str] | None = None,
    vertical_measurement_description_prefix: str | None = None,
    vertical_measurement_id: str | list[str] | None = None,
    vertical_measurement_id_prefix: str | None = None,
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
    if rig_id and isinstance(rig_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RigID"), value=rig_id))
    if rig_id and isinstance(rig_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RigID"), values=rig_id))
    if rig_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RigID"), value=rig_id_prefix))
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
    if vertical_measurement_id and isinstance(vertical_measurement_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VerticalMeasurementID"), value=vertical_measurement_id)
        )
    if vertical_measurement_id and isinstance(vertical_measurement_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalMeasurementID"), values=vertical_measurement_id))
    if vertical_measurement_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("VerticalMeasurementID"), value=vertical_measurement_id_prefix)
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
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
